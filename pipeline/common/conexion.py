"""
Utilidad compartida para obtener parametros de conexion.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Ruta fija, no relativa al directorio de trabajo
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _env(clave: str, defecto: str) -> str:
    return os.environ.get(clave, defecto)


TIMESCALE = "timescale"
POSTGRES = "postgres"

# Topicos de Kafka
TOPIC_RAW = _env("KAFKA_TOPIC_RAW", "iot.telemetry.raw")
TOPIC_DLQ = _env("KAFKA_TOPIC_DLQ", "iot.telemetry.dlq")

# Particiones y factor de replica al (re)crear un topico
NUM_PARTITIONS = int(_env("KAFKA_NUM_PARTITIONS", "3"))
REPLICATION_FACTOR = int(_env("KAFKA_DEFAULT_REPLICATION_FACTOR", "1"))

# Listener EXTERNAL de Kafka: el interno (kafka:9092) solo resuelve desde dentro
# de la red de contenedores.
BOOTSTRAP_SERVERS = f"localhost:{_env('KAFKA_EXTERNAL_PORT', '29092')}"


_BD = {
    TIMESCALE: {
        "host": "localhost",
        "port": int(_env("TIMESCALE_PORT", "5432")),
        "dbname": _env("TIMESCALE_DB", "tfm_metrics"),
        "user": _env("TIMESCALE_USER", "tfm"),
        "password": _env("TIMESCALE_PASSWORD", "tfm_dev_password"),
    },
    POSTGRES: {
        "host": "localhost",
        "port": int(_env("POSTGRES_PORT", "5433")),
        "dbname": _env("POSTGRES_DB", "tfm_analytics"),
        "user": _env("POSTGRES_USER", "tfm"),
        "password": _env("POSTGRES_PASSWORD", "tfm_dev_password"),
    },
}


def props_bd(cual: str) -> dict:
    """Parametros con las claves que espera psycopg2.connect, para el sumidero `cual`."""
    try:
        return dict(_BD[cual])  # copia: el llamante no muta el dict compartido
    except KeyError:
        raise ValueError(f"Base de datos desconocida: {cual!r}") from None
