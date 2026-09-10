"""
Resolucion del esquema contra Apicurio (API compatible con Confluent) y formato
de los eventos.

El productor serializa con el `AvroSerializer` de confluent-kafka, que escribe el
formato del ecosistema Kafka:

    [ 1 byte  ] byte magico 0x00
    [ 4 bytes ] id del esquema en el registro
    [ resto   ] payload Avro binario schemaless

NO CONFUNDIR CON `pipeline/schemas/register_schema.py`. Aquel es el script que
REGISTRA el contrato y sus reglas, y se ejecuta cuando el esquema cambia.
Este es la biblioteca que RESUELVE el esquema para leer/escribir, y la importan
el bridge, el job de Spark y las herramientas de medida en cada arranque.
"""

import os
import struct

from confluent_kafka.schema_registry import SchemaRegistryClient

from common.conexion import TOPIC_RAW

# Desde el host, Apicurio se ve en localhost; desde un contenedor de la red de
# Compose, en `http://apicurio:8080`. La variable APICURIO_URL permite fijarlo
# sin argumentos (la usa el servicio register-schema del docker-compose).
DEFAULT_REGISTRY_URL = os.environ.get("APICURIO_URL", "http://localhost:8080")

# Subject del proyecto. Describe el VALOR de los mensajes del topico)
DEFAULT_SUBJECT = f"{TOPIC_RAW}-value"

# Formato de Confluent: byte magico 0x00 + id de esquema de 4 bytes
# antes del payload Avro schemaless.
MAGIC_BYTE = 0x00
HEADER_FORMAT = ">BI"  # 1 byte magico + 4 bytes de id
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class SchemaRegistryError(RuntimeError):
    """No se pudo resolver un esquema contra el registro."""


def ccompat_url(base_url: str = DEFAULT_REGISTRY_URL) -> str:
    """Endpoint compatible con Confluent del registro."""
    return f"{base_url.rstrip('/')}/apis/ccompat/v7"


def schema_registry_client(base_url: str = DEFAULT_REGISTRY_URL) -> SchemaRegistryClient:
    """Cliente del registro que habla el protocolo de Confluent."""
    return SchemaRegistryClient({"url": ccompat_url(base_url)})


def latest_schema(client: SchemaRegistryClient,
                  subject: str = DEFAULT_SUBJECT) -> tuple[int, str]:
    """Devuelve (schema_id, schema_str) de la ultima version del subject.

    Se resuelve UNA vez al arrancar: adoptar una version nueva se hace
    reiniciando el servicio.
    """
    try:
        rs = client.get_latest_version(subject)
    except Exception as exc:
        raise SchemaRegistryError(
            f"No se pudo resolver el subject '{subject}' en el registro. "
            "Registralo antes: python pipeline/schemas/register_schema.py"
        ) from exc
    return rs.schema_id, rs.schema.schema_str


def all_schemas(client: SchemaRegistryClient,
                subject: str = DEFAULT_SUBJECT) -> dict[int, str]:
    """Devuelve {schema_id: schema_str} de TODAS las versiones registradas del subject.

    El consumidor (Spark) decodifica cada mensaje con el esquema con el que se
    escribio: la cabecera de cable lleva el schema_id, y con este mapa el job
    elige el `from_avro` correcto por fila. Asi varias versiones del contrato
    pueden convivir en el topico y el productor y el consumidor se despliegan
    sin coordinar una ventana comun. Se resuelve UNA vez al arrancar.
    """
    try:
        versiones = client.get_versions(subject)
    except Exception as exc:
        raise SchemaRegistryError(
            f"No se pudieron listar las versiones del subject '{subject}' en el registro. "
            "Registralo antes: python pipeline/schemas/register_schema.py"
        ) from exc
    if not versiones:
        raise SchemaRegistryError(
            f"El subject '{subject}' no tiene ninguna version registrada. "
            "Registralo antes: python pipeline/schemas/register_schema.py")
    esquemas: dict[int, str] = {}
    for v in versiones:
        rv = client.get_version(subject, v)
        esquemas[rv.schema_id] = rv.schema.schema_str
    return esquemas


def encode_header(schema_id: int) -> bytes:
    """Construye la cabecera de 5 bytes del formato de Confluent.
    """
    return struct.pack(HEADER_FORMAT, MAGIC_BYTE, schema_id)
