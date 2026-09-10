"""
demo.py — Demostracion en vivo para la defensa del proyecto.

No mide KPIs: orquesta las piezas que ya existen en el orden correcto para una 
demostracion. Levanta el stack, deja las bases limpias, arranca Spark + simulador

Los datos son REUBICADOS AL PRESENTE por `prepare_ashrae.py --fecha-final`

Uso:
    python pipeline/tools/demo.py                      # 6 semanas, acelerar 8000
    python pipeline/tools/demo.py --semanas 10         # 10 semanas hacia atras
    python pipeline/tools/demo.py --semanas 10 --acelerar 4000
    python pipeline/tools/demo.py --stop               # cierra todo

Ejecutar desde el venv del proyecto (usa confluent-kafka, aiomqtt, pyspark).
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging
from common.conexion import POSTGRES, TIMESCALE, props_bd

logger = logging.getLogger("demo")

# --- Rutas de las piezas que se orquestan ---------------------------------
COMPOSE = RAIZ / "docker-compose.yml"
SIMULADOR = RAIZ / "simulator" / "mqtt_simulator.py"
SPARK = RAIZ / "spark" / "stream_processing.py"
RESET = RAIZ / "tools" / "reset_state.py"
SPARK_LOG = RAIZ / "logs" / "spark_job.log"
ESTADO = Path(DIRECTORIO_LOGS) / "demo_estado.json"  # PIDs de los procesos en marcha

# --- Constantes -----------------------------------------------------------
APICURIO_INFO = "http://localhost:8080/apis/registry/v3/system/info"
GRAFANA_URL = "http://localhost:3000"
_PG = props_bd(POSTGRES)
_TS = props_bd(TIMESCALE)
SUMIDEROS = [("tfm-postgres", _PG["dbname"]), ("tfm-timescaledb", _TS["dbname"])]


class DemoError(RuntimeError):
    """Fallo en la orquestacion de la demo."""


# --------------------------------------------------------------------------
# Utilidades
# --------------------------------------------------------------------------
def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", "-f", str(COMPOSE), *args], check=check)


def esperar(cond, timeout: float, intervalo: float, desc: str) -> None:
    """Sondea `cond()` hasta que sea verdadera o se agote el tiempo."""
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        if cond():
            return
        time.sleep(intervalo)
    raise DemoError(f"Tiempo agotado esperando: {desc}")


def apicurio_ok() -> bool:
    try:
        with urllib.request.urlopen(APICURIO_INFO, timeout=5) as r:
            return r.status == 200
    except Exception:
        return False


def pg_ok(contenedor: str, db: str) -> bool:
    r = subprocess.run(["docker", "exec", contenedor, "pg_isready", "-U", _PG["user"], "-d", db],
                       capture_output=True)
    return r.returncode == 0


def contar_postgres() -> int:
    r = subprocess.run(
        ["docker", "exec", "tfm-postgres", "psql", "-U", _PG["user"], "-d", _PG["dbname"],
         "-tAc", "SELECT count(*) FROM telemetry_events;"],
        capture_output=True, text=True)
    try:
        return int(r.stdout.strip())
    except (ValueError, AttributeError):
        return -1


def lanzar_fondo(nombre: str, orden: list[str]) -> subprocess.Popen:
    """Arranca un proceso en segundo plano con el python del venv (sys.executable)."""
    salida = open(Path(DIRECTORIO_LOGS) / f"{nombre}_demo.out", "w")
    proc = subprocess.Popen([sys.executable, *orden], stdout=salida, stderr=subprocess.STDOUT)
    logger.info("Arrancado %s (pid=%d)", nombre, proc.pid)
    return proc


def esperar_spark(proc: subprocess.Popen, offset: int, timeout: float = 180.0) -> None:
    """Espera a que Spark escriba 'Consultas en marcha' desde `offset` del log.

    No se arranca el simulador hasta que las dos consultas de streaming estan activas.
    """
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        if proc.poll() is not None:
            raise DemoError(f"Spark murio al arrancar (codigo {proc.returncode}). "
                            f"Revisa logs/spark_demo.out y logs/spark_job.log")
        if SPARK_LOG.exists():
            tam = SPARK_LOG.stat().st_size
            desde = 0 if tam < offset else offset  # el log rotó
            with open(SPARK_LOG, "r", errors="replace") as f:
                f.seek(desde)
                if "Consultas en marcha" in f.read():
                    return
        time.sleep(2)
    raise DemoError("Spark no llego a 'Consultas en marcha' a tiempo")


# --------------------------------------------------------------------------
# Estado (PIDs) para poder cerrar con --stop
# --------------------------------------------------------------------------
def guardar_estado(pids: dict) -> None:
    ESTADO.write_text(json.dumps(pids), encoding="utf-8")


def cargar_estado() -> dict:
    try:
        return json.loads(ESTADO.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}


def vivo(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def hay_demo_en_marcha() -> bool:
    return any(vivo(pid) for pid in cargar_estado().values())


# --------------------------------------------------------------------------
# Arranque
# --------------------------------------------------------------------------
def run_start(args: argparse.Namespace) -> int:
    if not 1 <= args.semanas <= 52:
        raise DemoError("--semanas debe estar entre 1 y 52")
    if hay_demo_en_marcha():
        raise DemoError("Ya hay una demo en marcha. Cierrala antes: "
                        "python pipeline/tools/demo.py --stop")

    logger.info("1/4 Levantando el stack Docker (incluye register-schema y el bridge)...")
    compose("up", "-d")
    esperar(apicurio_ok, timeout=120, intervalo=3, desc="Apicurio")
    for cont, db in SUMIDEROS:
        esperar(lambda c=cont, d=db: pg_ok(c, d), timeout=60, intervalo=3, desc=cont)
    logger.info("    Stack sano (Apicurio + TimescaleDB + PostgreSQL); esquema registrado y bridge arriba")

    logger.info("2/4 Dejando las bases limpias (reset_state)...")
    if subprocess.run([sys.executable, str(RESET), "--yes"]).returncode != 0:
        raise DemoError("reset_state fallo")

    logger.info("3/4 Arrancando Spark y esperando a las consultas de streaming...")
    offset = SPARK_LOG.stat().st_size if SPARK_LOG.exists() else 0
    spark = lanzar_fondo("spark", [str(SPARK), "--trigger", "1 second"])
    esperar_spark(spark, offset)
    logger.info("    Spark en marcha con las dos consultas activas")

    eta_min = args.semanas * 7 * 24 * 3600 / args.acelerar / 60
    logger.info("4/4 Arrancando el simulador: cola de %d semanas (--ultimas-semanas), "
                "--acelerar %g", args.semanas, args.acelerar)
    sim = lanzar_fondo("simulador", [
        str(SIMULADOR), "--acelerar", str(args.acelerar),
        "--ultimas-semanas", str(args.semanas)])

    guardar_estado({"spark": spark.pid, "simulador": sim.pid})

    logger.info("Esperando a que lleguen los primeros datos...")
    esperar(lambda: contar_postgres() > 0, timeout=90, intervalo=3, desc="primeros datos")

    _resumen(args, eta_min)
    return 0


# Presets REALES del selector de tiempo de Grafana que cubren rangos de dias
PRESETS_GRAFANA = [(7, "Last 7 days"), (30, "Last 30 days"), (90, "Last 90 days"),
                   (180, "Last 6 months"), (365, "Last 1 year"), (730, "Last 2 years")]


def _preset_grafana(dias: float) -> str:
    for umbral, etiqueta in PRESETS_GRAFANA:
        if dias <= umbral:
            return etiqueta
    return "Last 5 years"


def _resumen(args: argparse.Namespace, eta_min: float) -> None:
    rango = f"now-{args.semanas + 1}w"
    preset = _preset_grafana((args.semanas + 1) * 7)
    print("\n" + "=" * 68)
    print("  DEMO LISTA — los datos ya estan fluyendo")
    print("=" * 68)
    print(f"  Grafana:   {GRAFANA_URL}")
    print("  Dashboards (menu ☰ -> Dashboards):")
    print("    1. Estado del pipeline   -> rango 'now-10m' (ingesta en vivo)")
    print(f"    2. Consumo energetico    -> rango '{rango}' o '{preset}'")
    print("    3. Calidad y anomalias   -> rango igual al Dashboard 2")
    print()
    print(f"  El replay de {args.semanas} semanas termina de llenarse en ~{eta_min:.0f} min")
    print("  Tiempo aproximado para que Grafana comience a mostrar datos: 1 minuto")
    print()
    print("  Para cerrar todo:  python pipeline/tools/demo.py --stop")
    print("=" * 68 + "\n")


# --------------------------------------------------------------------------
# Cierre
# --------------------------------------------------------------------------
def run_stop() -> int:
    logger.info("Deteniendo los procesos del pipeline...")
    estado = cargar_estado()
    if not estado:
        logger.info("Sin estado guardado; intento por nombre de script")
        for patron in ("mqtt_simulator.py", "stream_processing.py"):
            subprocess.run(["pkill", "-9", "-f", patron])
    else:
        for nombre, pid in estado.items():
            if vivo(pid):
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass
        time.sleep(3)
        for nombre, pid in estado.items():
            if vivo(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
                logger.info("Forzado el cierre de %s (pid=%d)", nombre, pid)
    ESTADO.unlink(missing_ok=True)

    logger.info("Bajando el stack Docker (se conservan los volumenes)...")
    compose("down", check=False)
    logger.info("Todo cerrado.")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prepara el terreno para la demo")
    p.add_argument("--semanas", type=int, default=6, metavar="1-52",
                   help="Semanas de datos hacia atras (por defecto 6)")
    p.add_argument("--acelerar", type=float, default=8000.0, metavar="FACTOR",
                   help="Factor de aceleracion del replay (por defecto 8000)")
    p.add_argument("--stop", action="store_true", help="Cierra la demo: procesos + stack")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("demo")
    args = parse_args()
    try:
        sys.exit(run_stop() if args.stop else run_start(args))
    except DemoError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Interrumpido. Para cerrar lo que haya quedado: demo.py --stop")
        sys.exit(130)
