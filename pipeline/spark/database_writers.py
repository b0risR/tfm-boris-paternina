"""
Escritura idempotente en los dos sumideros
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
import time as _time

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


COLUMNAS_INSTRUMENTACION = ("sim_publish_ts", "persisted_at")


def _deduplicar(filas: list, columnas: list[str], claves: list[str]) -> tuple[list, list]:
    """Quita las claves repetidas.
    """
    indices_clave = [columnas.index(c) for c in claves]
    indices_dominio = [i for i, c in enumerate(columnas)
                       if c not in claves and c not in COLUMNAS_INSTRUMENTACION]

    por_clave: dict[tuple, object] = {}
    colisiones: list[tuple] = []
    for fila in filas:
        clave = tuple(fila[i] for i in indices_clave)
        previa = por_clave.get(clave)
        if previa is not None:
            medida_previa = tuple(previa[i] for i in indices_dominio)
            medida_actual = tuple(fila[i] for i in indices_dominio)
            if medida_previa != medida_actual:
                colisiones.append((clave, (medida_previa, medida_actual)))
        por_clave[clave] = fila
    return list(por_clave.values()), colisiones


def make_upsert_writer(props: dict, table: str, conflict_cols: list[str],
                       reintentos: int = 5, espera_reintento: float = 3.0):
    """Devuelve una funcion foreachBatch que hace UPSERT en PostgreSQL.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        columnas = batch_df.columns
        filas, colisiones = _deduplicar(batch_df.collect(), columnas, conflict_cols)
        if not filas:
            return

        for clave, medidas in colisiones:
            logger.error("[batch %d] COLISION DE CLAVE en %s: %s comparte %s con "
                         "medidas distintas %s. Se escribe una y la otra SE PIERDE",
                         batch_id, table, clave, conflict_cols, medidas)

        def a_utc(valor):
            """Marca explicitamente como UTC los datetime sin zona.
            """
            if isinstance(valor, datetime) and valor.tzinfo is None:
                return valor.replace(tzinfo=timezone.utc)
            return valor

        valores = [tuple(a_utc(v) for v in fila) for fila in filas]

        actualizables = [c for c in columnas if c not in conflict_cols]

        sql = (
            f"INSERT INTO {table} ({', '.join(columnas)}) VALUES %s "
            f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in actualizables)
            + ", persisted_at = now()"
        )

        # REINTENTOS ANTE UN FALLO DE LA BASE DE DATOS
        for intento in range(1, reintentos + 1):
            conn = None
            try:
                conn = psycopg2.connect(
                    host=props["host"], port=props["port"], dbname=props["dbname"],
                    user=props["user"], password=props["password"],
                )
                with conn, conn.cursor() as cur:
                    execute_values(cur, sql, valores, page_size=500)
                logger.info("[batch %d] %d filas -> %s", batch_id, len(filas), table)
                return
            except psycopg2.OperationalError as exc:
                if intento == reintentos:
                    logger.error("[batch %d] %s sigue inaccesible tras %d intentos: %s",
                                 batch_id, table, reintentos, exc)
                    raise
                logger.warning("[batch %d] %s inaccesible (intento %d/%d), reintento en "
                               "%.0f s: %s", batch_id, table, intento, reintentos,
                               espera_reintento, str(exc).strip().splitlines()[0])
                _time.sleep(espera_reintento)
            finally:
                if conn is not None:
                    conn.close()

    return write_batch


def load_reference_tables(spark: SparkSession, dim_path: Path, base_path: Path,
                          props: dict) -> None:
    """Vuelca las dos tablas de referencia en PostgreSQL al arrancar.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    def entero(v):
        # year_built y floor_count llegan como float.
        return None if v is None else int(v)

    edificios = [(r.building_id, r.site_id, r.primary_use,
                  entero(r.square_feet), entero(r.year_built), entero(r.floor_count))
                 for r in spark.read.parquet(str(dim_path)).collect()]
    baseline = [(r.building_id, r.meter_type, r.baseline_p25, r.baseline_p50,
                 r.baseline_p75, r.baseline_iqr)
                for r in spark.read.parquet(str(base_path)).collect()]

    conn = psycopg2.connect(host=props["host"], port=props["port"], dbname=props["dbname"],
                            user=props["user"], password=props["password"])
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO buildings
                    (building_id, site_id, primary_use, square_feet, year_built, floor_count)
                VALUES %s
                ON CONFLICT (building_id) DO UPDATE SET
                    site_id = EXCLUDED.site_id, primary_use = EXCLUDED.primary_use,
                    square_feet = EXCLUDED.square_feet, year_built = EXCLUDED.year_built,
                    floor_count = EXCLUDED.floor_count
            """, edificios, page_size=500)
            execute_values(cur, """
                INSERT INTO sensor_baseline
                    (building_id, meter_type, baseline_p25, baseline_p50,
                     baseline_p75, baseline_iqr)
                VALUES %s
                ON CONFLICT (building_id, meter_type) DO UPDATE SET
                    baseline_p25 = EXCLUDED.baseline_p25, baseline_p50 = EXCLUDED.baseline_p50,
                    baseline_p75 = EXCLUDED.baseline_p75, baseline_iqr = EXCLUDED.baseline_iqr
            """, baseline, page_size=500)
        logger.info("Referencia cargada en PostgreSQL: %d edificios, %d lineas base",
                    len(edificios), len(baseline))
    finally:
        conn.close()
