"""
Deja el pipeline en el estado limpio para medicion de KPI.

Todo KPI publicado en la memoria del trabajo se obtuvo "sobre el stack
completo, estado limpio (topico recreado, tablas vacias, checkpoints borrados)".

Uso:
    python reset_state.py                # pide confirmacion
    python reset_state.py --yes          # sin preguntar
    python reset_state.py --yes --all    # borra tablas de dimensiones y cuartiles
"""

import argparse
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.conexion import (
    NUM_PARTITIONS,
    POSTGRES,
    REPLICATION_FACTOR,
    TIMESCALE,
    TOPIC_DLQ,
    TOPIC_RAW,
    props_bd,
)
from common.logging_setup import configurar_logging

logger = logging.getLogger("reset_state")

CONTENEDOR_KAFKA = "tfm-kafka"
KAFKA_TOPICS = "/opt/kafka/bin/kafka-topics.sh"
BOOTSTRAP_INTERNO = "kafka:9092"
COMPOSE = Path(__file__).resolve().parents[1] / "docker-compose.yml"

DIRECTORIO_CHECKPOINTS = Path(__file__).resolve().parents[1] / "spark" / "checkpoints"

PROCESOS_INCOMPATIBLES = ("stream_processing.py", "mqtt_simulator.py")

TABLAS_MEDICION = {
    TIMESCALE: ["telemetry_metrics", "streaming_progress"],
    POSTGRES: ["telemetry_events"],
}

TABLAS_REFERENCIA = {POSTGRES: ["buildings", "sensor_baseline"]}


# --------------------------------------------------------------------------
def procesos_en_marcha() -> list[str]:
    """Devuelve los procesos del pipeline que siguen vivos."""
    vivos = []
    for nombre in PROCESOS_INCOMPATIBLES:
        r = subprocess.run(["pgrep", "-f", nombre], capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            vivos.append(f"{nombre} (pid {', '.join(r.stdout.split())})")
    return vivos


def bridge_contenedor_en_marcha() -> bool:
    r = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "ps", "-q", "--status=running", "bridge"],
        capture_output=True, text=True,
    )
    return bool(r.stdout.strip())


def bridge_contenedor(accion: str) -> None:
    subprocess.run(["docker", "compose", "-f", str(COMPOSE), accion, "bridge"], check=False)


def kafka_topics(*argumentos: str) -> str:
    """Ejecuta kafka-topics.sh dentro del contenedor del broker.

    En el arranque normal los topicos se auto-crean; aqui se borran y recrean para
    dejar el estado limpio que exige una medicion.
    """
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, KAFKA_TOPICS,
         "--bootstrap-server", BOOTSTRAP_INTERNO, *argumentos],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"kafka-topics.sh fallo: {r.stderr.strip() or r.stdout.strip()}")
    return r.stdout


def recrear_topico(topico: str) -> None:
    particiones, replicas = NUM_PARTITIONS, REPLICATION_FACTOR

    if topico in kafka_topics("--list").split():
        logger.info("Borrando %s", topico)
        kafka_topics("--delete", "--topic", topico)
    else:
        logger.info("El topico %s no existia; se creara con %d particiones y replica %d",
                    topico, particiones, replicas)

    # El borrado es asincrono: crear de inmediato falla con TopicExistsException.
    for _ in range(60):
        if topico not in kafka_topics("--list").split():
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"El topico {topico} sigue existiendo 30 s despues de borrarlo")

    kafka_topics("--create", "--topic", topico,
                 "--partitions", str(particiones), "--replication-factor", str(replicas))
    logger.info("Recreado %s con %d particiones", topico, particiones)


def truncar(props: dict, tablas: list[str]) -> None:
    """Vacia las tablas indicadas informando de cuantas filas habia.
    """
    import psycopg2

    conn = psycopg2.connect(**props)
    try:
        with conn, conn.cursor() as cur:
            for tabla in tablas:
                cur.execute(f"SELECT count(*) FROM {tabla}")
                antes = cur.fetchone()[0]
                cur.execute(f"TRUNCATE TABLE {tabla}")
                logger.info("  %-20s %s filas borradas", tabla, f"{antes:,}")
    finally:
        conn.close()


def borrar_checkpoints() -> None:
    if not DIRECTORIO_CHECKPOINTS.exists():
        logger.info("No hay checkpoints que borrar en %s", DIRECTORIO_CHECKPOINTS)
        return
    subdirs = sorted(d.name for d in DIRECTORIO_CHECKPOINTS.iterdir() if d.is_dir())
    shutil.rmtree(DIRECTORIO_CHECKPOINTS)
    logger.info("Checkpoints borrados: %s", ", ".join(subdirs) or "(vacio)")


# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    vivos = procesos_en_marcha()
    if vivos:
        logger.error("Hay procesos del pipeline en marcha; detenlos antes de continuar:")
        for v in vivos:
            logger.error("  - %s", v)
        return 1

    tablas = {cual: list(t) for cual, t in TABLAS_MEDICION.items()}
    if args.all:
        for cual, extra in TABLAS_REFERENCIA.items():
            tablas[cual].extend(extra)

    logger.warning("Se va a BORRAR de forma irreversible:")
    logger.warning("  checkpoints : %s", DIRECTORIO_CHECKPOINTS)
    logger.warning("  topicos     : %s, %s", TOPIC_RAW, TOPIC_DLQ)
    for cual, lista in tablas.items():
        logger.warning("  %-12s: %s", cual, ", ".join(lista))

    if not args.yes:
        try:
            if input("Escribe 'si' para continuar: ").strip().lower() != "si":
                logger.info("Cancelado; no se ejecuto")
                return 1
        except EOFError:
            logger.error("Sin terminal interactiva: usa --yes para continuar")
            return 1

    # El bridge es un productor del topico que se va a recrear. Se para antes y
    # se reanuda despues, solo si estaba en marcha.
    reanudar_bridge = bridge_contenedor_en_marcha()
    if reanudar_bridge:
        logger.info("Deteniendo el contenedor del bridge para recrear los topicos...")
        bridge_contenedor("stop")

    borrar_checkpoints()
    recrear_topico(TOPIC_RAW)
    recrear_topico(TOPIC_DLQ)
    for cual, lista in tablas.items():
        logger.info("Truncando en %s:", cual)
        truncar(props_bd(cual), lista)

    if reanudar_bridge:
        logger.info("Reanudando el contenedor del bridge...")
        bridge_contenedor("start")

    logger.info("--- Estado limpio: el pipeline puede arrancar para una medicion nueva ---")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deja el pipeline en estado limpio")
    p.add_argument("--yes", action="store_true", help="No pedir confirmacion")
    p.add_argument("--all", action="store_true",
                   help="Truncar tambien buildings y sensor_baseline.")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("reset_state")
    try:
        sys.exit(run(parse_args()))
    except (RuntimeError, OSError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
