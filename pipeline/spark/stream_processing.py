"""
Job de Spark Structured Streaming.

Lee el topico de kafka iot.telemetry.raw con dos consultas, deserializa avro
y lo escribe en dos destinos:

  * TimescaleDB <- metricas agregadas por ventana temporal, para los dashboards
    operativos de Grafana.
  * PostgreSQL  <- eventos individuales enriquecidos, para los informes
    analiticos de Power BI.

Uso:
    python stream_processing.py                      # ambos sumideros
    python stream_processing.py --sink metrics       # solo TimescaleDB

El progreso de cada micro-lote se persiste en la tabla `streaming_progress` de
TimescaleDB.
"""

import argparse
import logging
import os
import sys
import time as _time
from pathlib import Path

# Se fuerza la zona horaria del PROCESO a UTC
os.environ["TZ"] = "UTC"
_time.tzset()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.apicurio import (
    DEFAULT_REGISTRY_URL,
    DEFAULT_SUBJECT,
    HEADER_SIZE,
    SchemaRegistryError,
    all_schemas,
    schema_registry_client,
)
from common.logging_setup import configurar_logging

# --- Configuracion de conexion ---------------------------
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

TIMESCALE = "timescale"
POSTGRES = "postgres"

BOOTSTRAP_SERVERS = os.environ["SPARK_KAFKA_BOOTSTRAP"]
TOPIC_RAW = os.environ.get("KAFKA_TOPIC_RAW", "iot.telemetry.raw")

_BD = {
    TIMESCALE: {
        "host": os.environ["SPARK_TS_HOST"],
        "port": int(os.environ["SPARK_TS_PORT"]),
        "dbname": os.environ.get("TIMESCALE_DB", "tfm_metrics"),
        "user": os.environ.get("TIMESCALE_USER", "tfm"),
        "password": os.environ["TIMESCALE_PASSWORD"],
    },
    POSTGRES: {
        "host": os.environ["SPARK_PG_HOST"],
        "port": int(os.environ["SPARK_PG_PORT"]),
        "dbname": os.environ.get("POSTGRES_DB", "tfm_analytics"),
        "user": os.environ.get("POSTGRES_USER", "tfm"),
        "password": os.environ["POSTGRES_PASSWORD"],
    },
}


def props_bd(cual: str) -> dict:
    """Parametros para psycopg2.connect del sumidero `cual` (TIMESCALE / POSTGRES)."""
    return dict(_BD[cual])

from spark.database_writers import (
    load_reference_tables,
    make_upsert_writer,
)
from spark.monitoring import (
    RegistroProgreso,
    supervisar,
)

logger = logging.getLogger("spark_job")

# Dependencias JVM que no vienen con PySpark. Se resuelven de Maven Central en
# el primer arranque y quedan en la cache local.
MAVEN_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    "org.apache.spark:spark-avro_2.13:4.2.0",
    "org.postgresql:postgresql:42.7.13",
]


def build_spark(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder
        .appName("tfm-telemetry-streaming")
        .master(args.master)
        .config("spark.jars.packages", ",".join(MAVEN_PACKAGES))
        # Zona horaria fija a UTC.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", args.starting_offsets)
        # failOnDataLoss ACTIVO por defecto: si la retencion de Kafka (7 dias)
        # borro offsets que el job aun no habia leido, la consulta se detiene en
        # vez de saltarselos.
        .option("failOnDataLoss", "true" if args.fail_on_data_loss else "false")
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
        .load()
    )


def _proyeccion_contrato(evento):
    """Reduce un struct decodificado al subconjunto de campos comun a todas las
    versiones del contrato.
    """
    return F.struct(
        evento["building_id"].alias("building_id"),
        evento["meter_type"].alias("meter_type"),
        evento["timestamp"].alias("timestamp"),
        evento["meter_reading"].alias("meter_reading"),
        evento["sim_publish_ts"].alias("sim_publish_ts"),
    )


def decode_events(raw: DataFrame, esquemas_por_id: dict[int, str]) -> DataFrame:
    """Quita la cabecera de cable de Confluent y decodifica el payload Avro
    eligiendo el esquema por el id que viaja en cada mensaje.

    La cabecera son 5 bytes: [byte magico 0x00][id de esquema de 4 bytes.
    Ese id selecciona con que esquema registrado se decodifica el
    payload: `esquemas_por_id` mapea cada schema_id del registro a su esquema
    Avro, resuelto al arrancar con `all_schemas`.

    UN schema_id NO REGISTRADO DETIENE EL JOB.
    """
    con_cabecera = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        # El id son los 4 bytes que siguen al byte magico (offset 2, longitud
        # HEADER_SIZE-1). conv(hex(...), 16, 10) los interpreta como entero
        F.conv(F.hex(F.substring(F.col("value"), 2, HEADER_SIZE - 1)), 16, 10)
            .cast("long").alias("schema_id"),
        F.expr(f"substring(value, {HEADER_SIZE + 1}, length(value) - {HEADER_SIZE})")
            .alias("avro_payload"),
    )

    # mode=FAILFAST: un payload que no encaje rompe el micro-lote en vez de
    # propagar una fila de nulos.
    ids = sorted(esquemas_por_id)
    items = list(esquemas_por_id.items())
    decodificado = _proyeccion_contrato(
        from_avro(F.col("avro_payload"), items[0][1], {"mode": "FAILFAST"}))
    for sid, sjson in items[1:]:
        decodificado = F.when(
            F.col("schema_id") == F.lit(int(sid)),
            _proyeccion_contrato(
                from_avro(F.col("avro_payload"), sjson, {"mode": "FAILFAST"})),
        ).otherwise(decodificado)

    con_evento = con_cabecera.withColumn("evento", decodificado)

    lectura_validada = F.when(
        F.col("schema_id").isin(ids),
        F.col("evento.meter_reading"),
    ).otherwise(
        F.raise_error(
            F.concat(
                F.lit("Evento con id de esquema no registrado: llego "),
                F.col("schema_id").cast("string"),
                F.lit(f"; registrados {ids}. El job se detiene en lugar de descartarlo; "),
                F.lit("el evento sigue en Kafka. Registra el esquema y reinicia el job "),
                F.lit("(consumidor antes que productor)."),
            )
        )
    )

    return con_evento.select(
        "kafka_key", "kafka_partition", "kafka_offset", "schema_id",
        F.col("evento.building_id").alias("building_id"),
        F.col("evento.meter_type").alias("meter_type"),
        F.col("evento.timestamp").alias("timestamp"),
        lectura_validada.alias("meter_reading"),
        F.col("evento.sim_publish_ts").alias("sim_publish_ts"),
    )


# --------------------------------------------------------------------------
# Datos de referencia (broadcast join)
# --------------------------------------------------------------------------
def load_reference(spark: SparkSession, dim_path: Path, base_path: Path):
    """Carga las dos tablas de referencia estaticas que enriquecen el flujo.
    """
    for p in (dim_path, base_path):
        if not p.exists():
            raise FileNotFoundError(
                f"No se encontro {p}. Genera los datos de referencia: "
                "python pipeline/data/prepare_ashrae.py")

    dimension = spark.read.parquet(str(dim_path)).select(
        "building_id", "site_id", "primary_use", "square_feet")
    linea_base = spark.read.parquet(str(base_path)).select(
        "building_id", "meter_type", "baseline_p75", "baseline_iqr")

    logger.info("Referencia cargada: %d edificios, %d sensores con linea base",
                dimension.count(), linea_base.count())
    return F.broadcast(dimension), F.broadcast(linea_base)


# --------------------------------------------------------------------------
# Enriquecimiento
# --------------------------------------------------------------------------
def enrich(df: DataFrame, dimension: DataFrame, linea_base: DataFrame) -> DataFrame:
    df = df.join(dimension, on="building_id", how="left")
    df = df.join(linea_base, on=["building_id", "meter_type"], how="left")

    lectura_cero = F.col("meter_reading") == 0
    pico_atipico = (
        F.col("baseline_iqr").isNotNull()
        & (F.col("baseline_iqr") > 0)
        & (F.col("meter_reading") > F.col("baseline_p75") + 5 * F.col("baseline_iqr"))
    )

    return (
        df.withColumn("is_zero_reading", lectura_cero)
        .withColumn("is_anomaly", pico_atipico)
    )


# --------------------------------------------------------------------------
# Agregacion por ventana
# --------------------------------------------------------------------------
def aggregate_metrics(df: DataFrame, window_duration: str, watermark: str,
                      margen_futuro: float) -> DataFrame:
    """Agrega por ventana temporal sobre EVENT TIME (columna `timestamp`).
    """
    df = df.filter(F.col("site_id").isNotNull())
    limite_futuro = F.current_timestamp().cast("double") + F.lit(float(margen_futuro))
    df = df.filter(F.col("timestamp").cast("double") <= limite_futuro)

    return (
        df.withWatermark("timestamp", watermark)
        .groupBy(
            F.window(F.col("timestamp"), window_duration),
            F.col("site_id"), F.col("primary_use"), F.col("meter_type"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("building_id").alias("distinct_buildings"),
            F.avg("meter_reading").alias("avg_reading"),
            F.max("meter_reading").alias("max_reading"),
            F.sum("meter_reading").alias("sum_reading"),
            F.sum("square_feet").alias("sum_square_feet"),
            F.sum(F.when(F.col("is_zero_reading"), 1).otherwise(0)).alias("zero_count"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "site_id", "primary_use", "meter_type",
            "event_count", "distinct_buildings",
            "avg_reading", "max_reading", "sum_reading",
            "sum_square_feet",
            F.when(F.col("sum_square_feet") > 0,
                   F.col("sum_reading") / F.col("sum_square_feet"))
             .alias("avg_energy_intensity"),
            "zero_count", "anomaly_count",
        )
    )


# --------------------------------------------------------------------------
# Escritura
# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    # all_schemas devuelve {schema_id: schema_str} de TODAS las versiones
    # registradas: el job decodifica cada mensaje con el esquema de su id.
    sr_client = schema_registry_client(args.registry_url)
    esquemas = all_schemas(sr_client, args.subject)

    spark = build_spark(args)
    spark.sparkContext.setLogLevel(args.spark_log_level)
    logger.info("Spark %s | esquemas registrados id=%s | ventana=%s watermark=%s margen_futuro=%.0fs",
                spark.version, sorted(esquemas), args.window, args.watermark, args.margen_futuro)

    eventos = decode_events(read_kafka(spark, args), esquemas)

    # El enriquecimiento se aplica UNA vez y lo comparten los dos sumideros
    dimension, linea_base = load_reference(spark, Path(args.buildings), Path(args.baseline))
    enriquecidos = enrich(eventos, dimension, linea_base)

    arrancadores = {}
    checkpoint_raiz = Path(args.checkpoint_dir).resolve()

    if args.sink in ("metrics", "both"):
        metricas = aggregate_metrics(enriquecidos, args.window, args.watermark,
                                     args.margen_futuro)
        arrancadores["metricas-timescaledb"] = lambda: (
            metricas.writeStream
            # outputMode append: emite cada ventana UNA vez, cuando el
            # watermark garantiza que ya no llegaran mas eventos suyos.
            .outputMode("append")
            .foreachBatch(make_upsert_writer(
                props_bd(TIMESCALE), "telemetry_metrics",
                ["window_start", "site_id", "primary_use", "meter_type"],
                args.db_retries, args.db_retry_wait))
            .option("checkpointLocation", str(checkpoint_raiz / "metrics"))
            .trigger(processingTime=args.trigger)
            .queryName("metricas-timescaledb")
            .start()
        )

    if args.sink in ("events", "both"):
        load_reference_tables(spark, Path(args.buildings), Path(args.baseline),
                              props_bd(POSTGRES))

        eventos_bd = enriquecidos.select(
            "building_id", "meter_type",
            F.col("timestamp").alias("event_time"),
            "meter_reading", "sim_publish_ts",
        )
        arrancadores["eventos-postgresql"] = lambda: (
            eventos_bd.writeStream
            .outputMode("append")
            .foreachBatch(make_upsert_writer(
                props_bd(POSTGRES), "telemetry_events",
                ["building_id", "meter_type", "event_time"],
                args.db_retries, args.db_retry_wait))
            .option("checkpointLocation", str(checkpoint_raiz / "events"))
            .trigger(processingTime=args.trigger)
            .queryName("eventos-postgresql")
            .start()
        )

    run_id = _time.strftime("%Y%m%dT%H%M%SZ", _time.gmtime())
    registro = None
    if args.progress_interval:
        props_progreso = props_bd(TIMESCALE)
        registro = RegistroProgreso([], props_progreso, run_id)
        registro.arrancar(args.progress_interval)
        logger.info("Progreso de micro-lote -> TimescaleDB.streaming_progress (run_id=%s)", run_id)

    try:
        codigo = supervisar(arrancadores, args.supervision_interval,
                            args.max_reinicios, registro)
    except KeyboardInterrupt:
        logger.info("Parada solicitada")
        codigo = 0
    finally:
        # Volcado final
        if registro:
            logger.info("Registrando el progreso de los ultimos lotes...")
            registro.volcar()
    return codigo


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job de Spark Structured Streaming")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    p.add_argument("--no-fail-on-data-loss", dest="fail_on_data_loss", action="store_false",
                   help="si la retencion de Kafka (7 dias) borro offsets que el job aun "
                        "no habia leido, la consulta se detiene en vez de saltarselos")
    p.add_argument("--max-offsets-per-trigger", type=int, default=10000,
                   help="Techo de eventos por micro-lote; acota la latencia del lote")
    p.add_argument("--window", default="1 hour",
                   help="Duracion de la ventana tumbling sobre event time")
    p.add_argument("--watermark", default="2 minutes",
                   help="Retraso maximo admitido antes de dar una ventana por cerrada")
    p.add_argument("--margen-futuro", type=float, default=300.0,
                   help="Segundos de adelanto tolerados antes de apartar un evento de la "
                        "agregacion")
    p.add_argument("--trigger", default="1 second",
                   help="Intervalo de micro-lote (KPI del Objetivo 3)")
    p.add_argument("--buildings", default=str(Path(__file__).parent / "../data/ashrae_buildings.parquet"),
                   help="Tabla de dimension de edificios (broadcast join)")
    p.add_argument("--baseline", default=str(Path(__file__).parent / "../data/ashrae_sensor_baseline.parquet"),
                   help="Linea base por sensor para la deteccion de picos (broadcast join)")
    p.add_argument("--shuffle-partitions", type=int, default=8)

    p.add_argument("--sink", default="both", choices=["both", "metrics", "events"],
                   help="Sumideros activos")
    p.add_argument("--checkpoint-dir", default=str(Path(__file__).parent / "checkpoints"))


    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--subject", default=DEFAULT_SUBJECT,
                   help="Subject del esquema en el registro (convencion {topic}-value)")
    p.add_argument("--spark-log-level", default="WARN")
    p.add_argument("--db-retries", type=int, default=5,
                   help="Intentos de escritura antes de dar por perdida la base de datos")
    p.add_argument("--db-retry-wait", type=float, default=3.0,
                   help="Segundos entre intentos de escritura")
    p.add_argument("--supervision-interval", type=float, default=5.0,
                   help="Segundos entre comprobaciones de que las consultas siguen vivas")
    p.add_argument("--max-reinicios", type=int, default=3,
                   help="Veces que se relanza una consulta caida antes de abandonarla")
    p.add_argument("--progress-interval", type=float, default=10.0,
                   help="Segundos entre volcados del progreso de micro-lote a "
                        "streaming_progress")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("spark_job")
    try:
        sys.exit(run(parse_args()))
    except SchemaRegistryError as exc:
        logger.error("%s", exc)
        sys.exit(1)
