"""
Prueba de recuperacion ante fallo de un servicio.

  1. Comprueba que bridge y job de Spark estan en marcha.
  2. Lanza el simulador a una tasa fija y espera a ver flujo llegando al sumidero.
  3. Provoca el fallo del contenedor elegido.
  4. Cronometra cuanto tarda el flujo en restablecerse contando filas nuevas en
     la base de datos: con kill, desde la orden de reinicio (con el arranque del
     contenedor incluido); con oom, desde el propio OOM.
  5. Con el flujo restablecido, ESPERA a que el simulador agote su --limite.
  6. Mide la tasa de perdida UNA sola vez, al final: los eventos que el simulador
     publico (valor de --limite) frente a las filas de telemetry_events.

Uso:
    python failover_test.py --target mosquitto            # fallo kill por defecto
    python failover_test.py --target postgres --fallo oom
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.conexion import (
    POSTGRES,
    TIMESCALE,
    props_bd,
)
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging
from common.stop_event import evento_de_parada

logger = logging.getLogger("failover_test")

RAIZ = Path(__file__).resolve().parents[1]
COMPOSE = RAIZ / "docker-compose.yml"
SIMULADOR = RAIZ / "simulator" / "mqtt_simulator.py"
# Un fichero para cada escenario (target, modo)
PATRON_RESULTADO = "failover_{target}_{modo}.json"

# Servicios que se pueden fallar en el pipeline
OBJETIVOS = ("mosquitto", "kafka", "timescaledb", "postgres", "bridge")

# Piezas que deben estar en marcha para que la prueba mida
PROCESOS_NECESARIOS = ("stream_processing.py",)
CONTENEDORES_NECESARIOS = ("bridge",)


def compose(*argumentos: str) -> None:
    r = subprocess.run(["docker", "compose", "-f", str(COMPOSE), *argumentos],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(argumentos)}: {r.stderr.strip()}")


def estado_contenedor(servicio: str) -> str:
    """Estado del contenedor como lo ve Docker: running, exited, healthy...
    """
    r = subprocess.run(["docker", "compose", "-f", str(COMPOSE), "ps", "--all",
                        "--format", "json", servicio],
                       capture_output=True, text=True)
    for linea in r.stdout.splitlines():
        if linea.strip():
            datos = json.loads(linea)
            return datos.get("Health") or datos.get("State") or "desconocido"
    return "ausente"


def _ids_contenedor(servicio: str) -> list[str]:
    r = subprocess.run(["docker", "compose", "-f", str(COMPOSE), "ps", "-q", servicio],
                       capture_output=True, text=True)
    return r.stdout.split()


def contenedor_id(servicio: str) -> str:
    """ID de la instancia del servicio a la que se ejecutara el fallo
    """
    ids = _ids_contenedor(servicio)
    if not ids:
        raise RuntimeError(f"no se encuentra el contenedor del servicio '{servicio}'")
    return ids[0]


def num_replicas(servicio: str) -> int:
    return len(_ids_contenedor(servicio))


def _update_memoria(cid: str, valor: str, reintentos: int = 50) -> bool:
    """`docker update --memory`, con reintentos."""
    for _ in range(reintentos):
        r = subprocess.run(
            ["docker", "update", "--memory", valor, "--memory-swap", valor, cid],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            return True
        time.sleep(0.3)
    return False


# Por debajo del RSS de todos los servicios salvo Mosquitto (que usa ~3 MiB, por
# debajo del minimo de 6 MB que admite Docker). Fuerza el OOM-kill del proceso 1.
MEM_OOM = "6m"


def forzar_oom(cid: str, timeout: float = 30.0) -> bool:
    """Baja el limite de memoria hasta que el kernel mata el proceso principal.
    """
    _update_memoria(cid, MEM_OOM)
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = subprocess.run(["docker", "inspect", "--format", "{{.State.OOMKilled}}", cid],
                           capture_output=True, text=True)
        if r.stdout.strip() == "true":
            return True
        time.sleep(0.3)
    return False


def procesos_ausentes() -> list[str]:
    faltan = []
    for nombre in PROCESOS_NECESARIOS:
        r = subprocess.run(["pgrep", "-f", nombre], capture_output=True, text=True)
        if r.returncode != 0:
            faltan.append(nombre)
    for servicio in CONTENEDORES_NECESARIOS:
        if estado_contenedor(servicio) not in ("healthy", "running"):
            faltan.append(f"contenedor {servicio}")
    return faltan


# Que tabla demuestra que el flujo volvio, segun el servicio que falle.
TABLA_TESTIGO = {
    "timescaledb": (TIMESCALE, "telemetry_metrics"),
    "postgres": (POSTGRES, "telemetry_events"),
    "mosquitto": (POSTGRES, "telemetry_events"),
    "kafka": (POSTGRES, "telemetry_events"),
    "bridge": (POSTGRES, "telemetry_events"),
}


def contar(props: dict, tabla: str) -> int:
    import psycopg2

    try:
        conn = psycopg2.connect(**props)
    except Exception:
        # Si el caido es el propio servidor, no poder conectar es lo esperado.
        return -1
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {tabla}")
            return cur.fetchone()[0]
    except Exception:
        return -1
    finally:
        conn.close()


def esperar_flujo(props: dict, tabla: str, referencia: int, timeout: float,
                  parada, desde: float | None = None) -> float | None:
    """Segundos hasta ver filas NUEVAS respecto a `referencia`, o None si no llegan.

    `desde` fija el origen del cronometro; por defecto es "ahora".
    """
    t0 = desde if desde is not None else time.monotonic()
    while time.monotonic() - t0 < timeout:
        if parada.is_set():
            return None
        actual = contar(props, tabla)
        if actual > referencia:
            return time.monotonic() - t0
        time.sleep(1)
    return None


def run(args: argparse.Namespace) -> int:
    parada = evento_de_parada("prueba de recuperacion")
    cual_bd, tabla = TABLA_TESTIGO[args.target]
    props = props_bd(cual_bd)
    # La tasa de perdida se mide sobre telemetry_events (PostgreSQL) 
    # frente a lo publicado en el topico de Kafka.
    props_pg = props_bd(POSTGRES)
    logger.info("Se observara el flujo en %s.%s, que es el camino que corta un fallo de %s",
                cual_bd, tabla, args.target)

    if args.fallo == "oom" and args.target == "mosquitto":
        logger.error("--fallo oom no sirve para mosquitto: usa ~3 MiB, por debajo del "
                     "minimo de 6 MB que admite `docker update --memory`. Usa --fallo kill.")
        return 1

    faltan = procesos_ausentes()
    if faltan:
        logger.error("Estos procesos deben estar en marcha para que la prueba mida algo:")
        for f in faltan:
            logger.error("  - %s", f)
        logger.error("Arranca el bridge y el job de Spark en otras terminales y repite")
        return 1

    # Limpieza al final del test, para quitar el limite de memoria que dejo `docker update`
    limpiar_memoria_al_final = False
    replicas_objetivo = num_replicas(args.target)  # >1 si se arranco con `--scale <target>=N`

    logger.info("Lanzando el simulador con speedup x%g en segundo plano...", args.speedup)
    simulador = subprocess.Popen(
        # La prueba debe arrancar de estado limpio: el watermark de
        # Spark es unidireccional, y un checkpoint ya avanzado de una corrida anterior
        # descartaria por tardio lo publicado despues, con lo que la prueba de
        # fallo sobre TimescaleDB no demostraria la prueba.
        [sys.executable, str(SIMULADOR), "--acelerar", str(args.speedup),
         "--limite", str(args.limit)],
    )
    recuperacion = None
    try:
        filas_inicio = contar(props, tabla)

        # telemetry_events en t~=0 (el simulador acaba de arrancar): el delta
        # hasta el final es todo lo que se persistio durante la prueba.
        eventos_pg_ini = contar(props_pg, "telemetry_events")

        if esperar_flujo(props, tabla, filas_inicio, args.warmup, parada) is None:
            logger.error("No llega flujo al sumidero antes del fallo; se aborta la prueba")
            return 1
        logger.info("Flujo confirmado. Filas antes del fallo: %s", f"{contar(props, tabla):,}")

        # Recuento de referencia
        filas_antes_del_fallo = contar(props, tabla)

        if args.fallo == "kill":
            logger.info("--- MATANDO %s con docker kill (filas antes: %s) ---",
                        args.target, f"{filas_antes_del_fallo:,}")
            compose("kill", args.target)
            instante_fallo = time.monotonic()
            logger.info("Estado de %s: %s", args.target, estado_contenedor(args.target))
            time.sleep(args.downtime)
            filas_durante = contar(props, tabla)
            logger.info("Filas tras %g s caido: %s", args.downtime,
                        f"{filas_durante:,}" if filas_durante >= 0 else "(base de datos caida)")
            logger.info("--- LEVANTANDO %s a mano ---", args.target)
            # El cronometro de recuperacion arranca antes de la orden de reinicio:
            # la cifra incluye lo que Docker tarda en volver a poner en pie el
            # contenedor.
            instante_reinicio = time.monotonic()
            compose("start", args.target)
        else:  # oom
            cid = contenedor_id(args.target)
            if replicas_objetivo > 1:
                logger.info("El servicio %s tiene %d replicas; se hace OOM de UNA "
                            "(%s). El flujo no deberia detenerse: las otras %d cubren "
                            "su parte.", args.target, replicas_objetivo, cid[:12],
                            replicas_objetivo - 1)
            logger.info("--- FORZANDO OOM de %s (limite -> %s; filas antes: %s) ---",
                        args.target, MEM_OOM, f"{filas_antes_del_fallo:,}")
            if not forzar_oom(cid):
                logger.error("No se consiguio forzar el OOM de %s en el plazo", args.target)
                return 1
            instante_fallo = time.monotonic()
            filas_durante = contar(props, tabla)
            # Se sube el limite a 2g para que el rearranque no vuelva a caer en OOM
            limpiar_memoria_al_final = True
            logger.info("OOM confirmado; subiendo el limite de memoria a 2g para que "
                        "el rearranque automatico prospere")
            instante_reinicio = time.monotonic()
            if not _update_memoria(cid, "2g"):
                logger.error("No se pudo subir el limite de memoria de %s", args.target)
                return 1

        # La referencia es el ULTIMO RECUENTO VALIDO.
        referencia = filas_durante if filas_durante >= 0 else filas_antes_del_fallo
        logger.info("Se esperan filas nuevas por encima de %s", f"{referencia:,}")
        recuperacion = esperar_flujo(props, tabla, referencia, args.timeout, parada,
                                     desde=instante_reinicio)
        total = time.monotonic() - instante_fallo

        if recuperacion is None:
            logger.error("EL FLUJO NO SE RESTABLECIO en %g s tras el fallo de %s",
                         args.timeout, args.target)
        elif args.fallo == "kill":
            logger.info("Flujo restablecido %.1f s despues de ordenar el reinicio "
                        "(incluye el arranque del contenedor; %.1f s desde el fallo)",
                        recuperacion, total)
        else:
            logger.info("Flujo restablecido %.1f s despues del OOM, con `restart: "
                        "unless-stopped` rearrancando el contenedor sin intervencion",
                        recuperacion)

    finally:
        if recuperacion is not None:
            # Recuperado el flujo: se deja que el simulador AGOTE --limite por su
            # cuenta.
            logger.info("Recuperado; esperando a que el simulador agote --limite=%d...",
                        args.limit)
            try:
                simulador.wait(timeout=args.limit / 20 + 300)
            except subprocess.TimeoutExpired:
                logger.warning("El simulador no agoto --limite en el plazo; se detiene")
                simulador.terminate()
        else:
            logger.info("Deteniendo el simulador...")
            simulador.terminate()
        try:
            simulador.wait(timeout=30)
        except subprocess.TimeoutExpired:
            simulador.kill()
        # El simulador sale 0 solo si publico toda su agenda (--limite); sale 1 si
        # abandono conexiones o se le paro antes. Sin ese 0 la tasa de perdida no
        # es concluyente y se deja en null.
        simulador_completo = simulador.returncode == 0
        if not simulador_completo:
            logger.warning("El simulador no completo --limite=%d (codigo %s); la tasa "
                           "de perdida no es concluyente", args.limit, simulador.returncode)

    # Drenaje: al pipeline aun le quedan mensajes en vuelo cuando el productor
    # para. Sin esta espera, la comparacion final contaria como perdido lo que
    # solo estaba en transito.
    logger.info("Esperando %g s al drenaje antes de contar...", args.drain)
    time.sleep(args.drain)

    # Tasa de perdida: --limite (todo lo que el simulador publico) frente a las
    # filas nuevas de telemetry_events. Si el simulador no agoto --limite, la
    # medicion no es concluyente y no se emite una cifra. Un delta negativo
    # (mas persistido que --limite) solo puede venir de un arranque no limpio;
    # se lleva a cero.
    persistidos = contar(props_pg, "telemetry_events") - eventos_pg_ini
    if simulador_completo:
        perdidos = max(args.limit - persistidos, 0)
        tasa_perdida_pct = round(perdidos / args.limit * 100, 4)
    else:
        perdidos, tasa_perdida_pct = None, None

    resultado = {
        "servicio": args.target,
        "replicas_objetivo": replicas_objetivo,
        "modo": args.fallo,
        "tabla_testigo": f"{cual_bd}.{tabla}",
        "instante": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "downtime_s": args.downtime if args.fallo == "kill" else None,
        "recuperacion_s": round(recuperacion, 1) if recuperacion is not None else None,
        "desde_el_fallo_s": round(total, 1),
        "simulador_completo": simulador_completo,
        "publicados": args.limit,
        "persistidos_pg": persistidos,
        "eventos_perdidos": perdidos,
        "tasa_perdida_pct": tasa_perdida_pct,
        "estado_final": estado_contenedor(args.target),
    }
    resultado_path = DIRECTORIO_LOGS / PATRON_RESULTADO.format(
        target=args.target, modo=args.fallo)
    resultado_path.write_text(json.dumps(resultado, indent=2))

    logger.info("--- Resultado de la prueba de recuperacion ---")
    for k, v in resultado.items():
        logger.info("  %-18s %s", k, v)
    if perdidos:
        logger.warning("PERDIDA DETECTADA: %d de %d eventos publicados no llegaron a "
                       "telemetry_events (%.4f%%)", perdidos, args.limit, tasa_perdida_pct)
    logger.info("Objetivo: recuperacion < 60 s sin perdida de datos")
    logger.info("Guardado en %s", resultado_path)

    # Este paso va AL final, despues de medir
    if limpiar_memoria_al_final:
        logger.info("Limpiando el limite de memoria de %s ",
                    args.target)
        cmd = ["docker", "compose", "-f", str(COMPOSE), "up", "-d",
               "--force-recreate", "--no-deps"]
        # Sin --scale, `up` dejaria el servicio escalado en 1 replica.
        if replicas_objetivo > 1:
            cmd += ["--scale", f"{args.target}={replicas_objetivo}"]
        cmd.append(args.target)
        subprocess.run(cmd, check=False)

    return 0 if recuperacion is not None and recuperacion < 60 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prueba de recuperacion ante fallo (TFM)")
    p.add_argument("--target", default="mosquitto", choices=OBJETIVOS,
                   help="Servicio que se va a tumbar")
    p.add_argument("--fallo", default="kill", choices=("kill", "oom"),
                   help="kill: docker kill + reinicio manual. oom no sirve para "
                        "mosquitto (usa ~3 MiB, por debajo del minimo de 6 MB de docker)")
    p.add_argument("--acelerar", dest="speedup", metavar="FACTOR", type=float, default=1000.0,
                   help="Aceleracion del reloj durante la prueba.")
    p.add_argument("--limite", dest="limit", metavar="N", type=int, default=20000,
                   help="Techo de eventos del simulador.")
    p.add_argument("--warmup", type=float, default=60.0,
                   help="Segundos maximos de espera hasta ver flujo antes del fallo")
    p.add_argument("--downtime", type=float, default=15.0,
                   help="Segundos que el servicio permanece caido")
    p.add_argument("--timeout", type=float, default=180.0,
                   help="Segundos maximos de espera a que el flujo se restablezca")
    p.add_argument("--drain", type=float, default=30.0,
                   help="Segundos de espera al drenaje antes del recuento final")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("failover_test")
    try:
        sys.exit(run(parse_args()))
    except (RuntimeError, OSError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
