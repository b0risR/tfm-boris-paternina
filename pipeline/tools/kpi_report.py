"""
Cuadro de KPIs del proyecto, medido sobre el estado del sistema.

Un solo comando que interroga a las fuentes donde el pipeline deja
constancia de lo hecho, y emite la tabla en Markdown.

De donde sale cada KPI:

  -latencia de ingesta y perdida    PostgreSQL + $SYS de Mosquitto + offsets DLQ de Kafka
  -validacion de esquema            Apicurio + topico DLQ
  -latencia de spark                TimescaleDB.streaming_progress
  -latencia de actualizacion
               de dashboards        API de consultas de Grafana
  -recuperacion ante fallo          lecturas de failover_*.json de failover_test.py

Para que las cifras sean fiables hay que partir de `reset_state.py`, generar la carga y ejecutarlo
al final.

Uso:
    python kpi_report.py
    python kpi_report.py --run-id 20260818T143000Z   # otra ejecucion del job
    python kpi_report.py --sin-grafana               # omite el Objetivo 4
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.apicurio import (
    DEFAULT_REGISTRY_URL,
    DEFAULT_SUBJECT,
    latest_schema,
    schema_registry_client,
)
from common.conexion import (
    POSTGRES,
    TIMESCALE,
    TOPIC_DLQ,
    TOPIC_RAW,
    props_bd,
)
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging

logger = logging.getLogger("kpi_report")

INFORME = DIRECTORIO_LOGS / "informe_kpi.md"
# failover_test.py deja un fichero por escenario (failover_<target>_<modo>.json);
# se agregan todos para la tabla del Objetivo 5.
PATRON_FAILOVER = "failover_*.json"
DIRECTORIO_DASHBOARDS = Path(__file__).resolve().parents[1] / "docker/grafana/dashboards"

CONTENEDOR_KAFKA = "tfm-kafka"
CONTENEDOR_MOSQUITTO = "tfm-mosquitto"


def consultar(props: dict, sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(**props)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------------
# -latencia de ingesta y perdida
# --------------------------------------------------------------------------
def kpi_ingesta(props_pg: dict) -> dict:
    """Latencia desde la publicacion hasta la persistencia final.
    """
    (persistidos, p50, p95, maximo, negativas, huerfanos), = consultar(props_pg, """
        SELECT count(*),
               percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (persisted_at - sim_publish_ts))),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (persisted_at - sim_publish_ts))),
               max(EXTRACT(EPOCH FROM (persisted_at - sim_publish_ts))),
               count(*) FILTER (WHERE persisted_at < sim_publish_ts),
               count(*) FILTER (WHERE b.building_id IS NULL)
        FROM telemetry_events e
        LEFT JOIN buildings b ON b.building_id = e.building_id
    """)

    return {"persistidos": persistidos, "p50": p50, "p95": p95,
            "max": maximo, "negativas": negativas, "huerfanos": huerfanos}


def offsets_kafka(topico: str) -> int:
    """Mensajes acumulados en un topico.
    """
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "kafka:9092", "--topic", topico, "--time", "-1"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"kafka-get-offsets.sh fallo: {r.stderr.strip()}")
    finales = sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.splitlines() if ":" in l)

    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "kafka:9092", "--topic", topico, "--time", "-2"],
        capture_output=True, text=True,
    )
    iniciales = sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.splitlines() if ":" in l)
    return finales - iniciales


def _leer_stat_mosquitto(topico_sys: str, timeout: int = 12) -> int | None:
    """Lee un contador de las estadisticas $SYS que publica Mosquitto.
    Son contadores acumulados desde que arranco el broker, publicados cada 10 s
    por defecto (de ahi el timeout).
    """
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_MOSQUITTO, "mosquitto_sub", "-h", "localhost",
         "-t", topico_sys, "-C", "1", "-W", str(timeout)],
        capture_output=True, text=True,
    )
    salida = r.stdout.strip()
    return int(salida) if salida.isdigit() else None


def mensajes_recibidos_mosquitto(timeout: int = 12) -> int | None:
    """Total de mensajes PUBLISH que Mosquitto recibio desde que arranco.
    """
    return _leer_stat_mosquitto("$SYS/broker/publish/messages/received", timeout)


def mensajes_descartados_mosquitto(timeout: int = 12) -> int | None:
    """Mensajes que el broker ACEPTO y nunca entrego, segun su propio medidor.
    """
    return _leer_stat_mosquitto("$SYS/broker/publish/messages/dropped", timeout)


# --------------------------------------------------------------------------
# -validacion de esquema
# --------------------------------------------------------------------------
def kpi_esquema(args) -> dict:
    sr_client = schema_registry_client(args.registry_url)
    _, schema_str = latest_schema(sr_client, args.subject)
    esquema = json.loads(schema_str)

    validos = offsets_kafka(TOPIC_RAW)
    invalidos = offsets_kafka(TOPIC_DLQ)
    total = validos + invalidos
    return {
        "nombre": f"{esquema.get('namespace', '')}.{esquema.get('name', '')}",
        "dlq": invalidos,
        "pct_validados": (validos / total * 100) if total else None,
    }


# --------------------------------------------------------------------------
# -latencia de spark
# --------------------------------------------------------------------------
def kpi_procesamiento(props_ts: dict, run_id: str | None) -> list[tuple]:
    """Duracion de microlote por consulta de spark sobre kafka.
    """
    if run_id is None:
        filas = consultar(props_ts, "SELECT max(run_id) FROM streaming_progress")
        run_id = filas[0][0] if filas else None
    if run_id is None:
        return []

    return consultar(props_ts, f"""
        SELECT query_name,
               count(*),
               percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms),
               max(duration_ms)
        FROM streaming_progress
        WHERE run_id = '{run_id}'
        GROUP BY query_name ORDER BY query_name
    """)


# --------------------------------------------------------------------------
# -latencia de actualizacion de dashboards
# --------------------------------------------------------------------------
DATASOURCE_POR_DEFECTO = {"type": "grafana-postgresql-datasource", "uid": "timescaledb"}


def _datasource_del_panel(target: dict, panel: dict) -> dict:
    """Fuente de datos que declara cada panel.
    """
    ds = target.get("datasource") or panel.get("datasource") or DATASOURCE_POR_DEFECTO
    return ds if isinstance(ds, dict) and ds.get("uid") else DATASOURCE_POR_DEFECTO


def kpi_grafana(url: str, usuario: str, clave: str, rango: str) -> list[dict]:
    """Cronometra cada consulta de panel por grafana.
    """
    medidas = []
    for fichero in sorted(DIRECTORIO_DASHBOARDS.glob("*.json")):
        panel_json = json.loads(fichero.read_text())
        for panel in panel_json.get("panels", []):
            objetivos = [t for t in panel.get("targets", []) if t.get("rawSql")]
            if not objetivos:
                continue

            cuerpo = {
                "from": rango, "to": "now",
                "queries": [{
                    "refId": t.get("refId", chr(65 + i)),
                    "datasource": _datasource_del_panel(t, panel),
                    "rawSql": t["rawSql"],
                    "format": t.get("format", "table"),
                    "rawQuery": True,
                    "intervalMs": 60000,
                    "maxDataPoints": 1000,
                } for i, t in enumerate(objetivos)],
            }
            t0 = time.monotonic()
            resp = requests.post(f"{url}/api/ds/query", json=cuerpo,
                                 auth=(usuario, clave), timeout=30)
            ms = (time.monotonic() - t0) * 1000
            medidas.append({
                "dashboard": panel_json.get("title", fichero.stem),
                "panel": panel.get("title", "(sin titulo)"),
                "ms": round(ms, 1),
                "ok": resp.status_code == 200,
            })
            if resp.status_code != 200:
                logger.warning("Grafana respondio %d al panel '%s': %s",
                               resp.status_code, panel.get("title"), resp.text[:200])
    return medidas


# --------------------------------------------------------------------------
# Elaboracion del informe
# --------------------------------------------------------------------------
def _num(valor, decimales=3):
    """Numero en convencion espanola: miles con punto y decimales con coma.
    """
    if valor is None:
        return "n/d"
    s = f"{valor:,.{decimales}f}"
    return s.replace(",", "\x00").replace(".", ",").replace("\x00", ".")


def _fmt(valor, decimales=3, sufijo=""):
    return "n/d" if valor is None else f"{_num(valor, decimales)}{sufijo}"


# Cada consulta de spark se nombra por el sumidero que alimenta.
FLUJOS = {
    "eventos-postgresql": "Eventos individuales (sumidero analitico)",
    "metricas-timescaledb": "Metricas agregadas (sumidero operacional)",
}


def construir_informe(ingesta, esquema, procesamiento, grafana, failovers) -> str:
    L = ["# Cuadro de indicadores de rendimiento", "",
         f"Generado el {time.strftime('%Y-%m-%d %H:%M:%S')} sobre el estado del sistema.",
         ""]

    # -- Objetivo 1 --------------------------------------------------------
    L += ["## Objetivo 1: Comprobar la persistencia de telemetria rapida y fiable", "",
          "| Indicador | Resultado | Objetivo |", "|---|---|---|",
          f"| Eventos persistidos | {_fmt(ingesta['persistidos'], 0)} | — |",
          f"| Eventos ingestados por Mosquitto | {_fmt(ingesta.get('publicados'), 0)} | — |",
          f"| Latencia de ingesta (mediana) | {_fmt(ingesta['p50'])} s | — |",
          f"| **Latencia de ingesta (percentil 95)** | **{_fmt(ingesta['p95'])} s** | < 2 s |",
          f"| Latencia de ingesta (maxima) | {_fmt(ingesta['max'])} s | — |",
          f"| **Tasa de perdida** | **{_fmt(ingesta.get('perdida_pct'), 4, ' %')}** | < 0,1 % |",
          ""]

    if ingesta.get("descartados"):
        L += [f"| Mensajes aceptados por el broker y no entregados | "
              f"{_fmt(ingesta['descartados'], 0)} | 0 |", "",
              "> Contador $SYS de Mosquitto (`messages/dropped`): mensajes que el broker "
              "confirmo al publicador y luego no entrego por tener llena la cola de salida de "
              "algun suscriptor. Es acumulado desde el arranque del contenedor de Mosquitto y "
              "solo es atribuible a esta corrida si se parte de `docker compose down -v`. Un "
              "valor alto con la tasa de perdida a 0 % suele ser la cola de una sesion "
              "persistente huerfana (un bridge de una recreacion previa), no perdida real.", ""]

    if ingesta.get("persistidos_de_mas"):
        L += ["> Hay mas eventos persistidos que publicados en el flujo porque la medicion se ha "
              "tomado sobre un estado que no estaba limpio. Conviene repetirla partiendo de un "
              "estado limpio.", ""]

    if ingesta.get("huerfanos"):
        L += [f"| Eventos sin edificio de referencia | {_fmt(ingesta['huerfanos'], 0)} | 0 |", "",
              "> Se persisten, pero quedan fuera de las metricas agregadas: el edificio no "
              "esta en la tabla de referencia.", ""]

    if ingesta["negativas"]:
        L += [f"> Aviso: {_fmt(ingesta['negativas'], 0)} eventos presentan una latencia "
              "negativa. La medicion no es fiable y debe repetirse.", ""]

    # -- Objetivo 2 ------------------------------------------------------
    if esquema:
        L += ["## Objetivo 2: Implementar la gobernanza del esquema de datos", "",
              "| Indicador | Resultado | Objetivo |", "|---|---|---|",
              f"| Esquema registrado | `{esquema['nombre']}` | — |",
              f"| Eventos validados contra el esquema | {_fmt(esquema['pct_validados'], 4, ' %')} | 100 % |",
              f"| Eventos con error de validacion | {_fmt(esquema['dlq'], 0)} | 0 |", ""]

    # -- Objetivo 3 ----------------------------------------------------
    L += ["## Objetivo 3: Procesar y enriquecer los datos en flujo con baja latencia", ""]
    if procesamiento:
        L += ["| Flujo de procesamiento | Micro-lotes | Duracion (mediana) | "
              "Duracion (percentil 95) | Duracion (maxima) | Objetivo |",
              "|---|---|---|---|---|---|"]
        for nombre, lotes, p50, p95, maximo in procesamiento:
            L.append(f"| {FLUJOS.get(nombre, nombre)} | {_fmt(lotes, 0)} | {_fmt(p50, 0, ' ms')} | "
                     f"**{_fmt(p95, 0, ' ms')}** | {_fmt(maximo, 0, ' ms')} | < 3 s |")
        L += [""]
    else:
        L += ["No hay datos de microlotes registrados para esta ejecucion.", ""]

    # -- Objetivo 4 --------------------------------------------------
    if grafana:
        L += ["## Objetivo 4: Ofrecer visualizacion operacional y analitica", "",
              "| Dashboard | Paneles | Latencia de actualizacion | Objetivo |",
              "|---|---|---|---|"]
        por_dashboard: dict[str, list[dict]] = {}
        for m in grafana:
            por_dashboard.setdefault(m["dashboard"], []).append(m)
        for titulo, paneles in por_dashboard.items():
            peor = max(paneles, key=lambda m: m["ms"])
            fallo = " (con errores)" if any(not m["ok"] for m in paneles) else ""
            L.append(f"| {titulo} | {len(paneles)} | {_num(peor['ms'], 0)} ms{fallo} | < 5 s |")
        L += [""]
        if any(not m["ok"] for m in grafana):
            L += ["> Aviso: algun panel devolvio error. Revisar el registro de actividad.", ""]

    # -- Objetivo 5 ------------------------------------------------
    # Una fila por escenario (failover_<target>_<modo>.json). Cada prueba falla
    # un servicio; se agregan aqui para dar el cuadro completo de resiliencia.
    if failovers:
        L += ["## Objetivo 5: Validar la resiliencia del sistema", ""]
        instantes = sorted(f["instante"] for f in failovers if f.get("instante"))
        if instantes:
            rango = instantes[0] if instantes[0] == instantes[-1] else \
                f"{instantes[0]} .. {instantes[-1]}"
            L += [f"{len(failovers)} escenario(s) de fallo, recogidos el {rango}. "
                  f"Objetivo: recuperacion < 60 s y tasa de perdida 0 %.", ""]
        L += ["| Servicio | `--fallo` | Interrupcion | Recuperacion | "
              "Tasa de perdida |", "|---|---|---|---|---|"]
        for fo in failovers:
            rec = fo.get("recuperacion_s")
            perd = fo.get("tasa_perdida_pct")
            m_rec = "✓" if rec is not None and rec < 60 else "✗"
            m_perd = "✓" if (perd or 0) == 0 else "✗"
            # downtime_s solo se escribe para --fallo kill; en un OOM es None
            dt = fo.get("downtime_s")
            interrupcion = (f"{dt:g} s (kill)" if dt is not None
                            else f"{_fmt(fo.get('desde_el_fallo_s'), 1, ' s')} desde el fallo")
            L.append(f"| `{fo.get('servicio', '—')}` | {fo.get('modo', '—')} | {interrupcion} | "
                     f"**{_fmt(rec, 1, ' s')}** {m_rec} | **{_fmt(perd, 4, ' %')}** {m_perd} |")
        L += [""]

    return "\n".join(L)


def run(args: argparse.Namespace) -> int:
    props_pg = props_bd(POSTGRES)
    props_ts = props_bd(TIMESCALE)

    logger.info("Objetivo 1: consultando latencias de ingesta...")
    ingesta = kpi_ingesta(props_pg)

    logger.info("Objetivo 1: leyendo lo ingestado por Mosquitto y lo invalidado en la DLQ...")
    ingestados = mensajes_recibidos_mosquitto()
    try:
        invalidos = offsets_kafka(TOPIC_DLQ)
    except RuntimeError as exc:
        logger.warning("No se pudo contar la DLQ en Kafka: %s", exc)
        invalidos = None

    ingesta["publicados"] = ingestados
    if ingestados and invalidos is not None:
        perdidos = ingestados - invalidos - ingesta["persistidos"]
        ingesta["perdida_pct"] = max(perdidos, 0) / ingestados * 100
        ingesta["persistidos_de_mas"] = perdidos < 0
    else:
        ingesta["perdida_pct"] = None
        ingesta["persistidos_de_mas"] = False

    logger.info("Objetivo 1: leyendo el medidor de descartes del broker...")
    ingesta["descartados"] = mensajes_descartados_mosquitto()

    esquema = None
    try:
        logger.info("Objetivo 2: consultando el registro de esquemas y los topicos...")
        esquema = kpi_esquema(args)
    except Exception as exc:
        logger.warning("No se pudo medir la gobernanza de esquema: %s", exc)

    logger.info("Objetivo 3: consultando el progreso de microlote...")
    procesamiento = kpi_procesamiento(props_ts, args.run_id)

    grafana = None
    if not args.sin_grafana:
        try:
            logger.info("Objetivo 4: cronometrando los paneles a traves de Grafana...")
            grafana = kpi_grafana(args.grafana_url, args.grafana_user,
                                  args.grafana_password, args.rango)
        except requests.RequestException as exc:
            logger.warning("No se pudo consultar Grafana: %s", exc)

    failovers = []
    for fichero in sorted(DIRECTORIO_LOGS.glob(PATRON_FAILOVER)):
        try:
            failovers.append(json.loads(fichero.read_text()))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Objetivo 5: no se pudo leer %s: %s", fichero.name, exc)
    if failovers:
        logger.info("Objetivo 5: %d escenario(s) de recuperacion ante fallo", len(failovers))
    else:
        logger.info("Objetivo 5: sin resultados de recuperacion ante fallo disponibles")

    informe = construir_informe(ingesta, esquema, procesamiento, grafana, failovers)
    INFORME.write_text(informe)
    print(informe)
    logger.info("Informe escrito en %s", INFORME)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cuadro de KPIs del pipeline (TFM)")
    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--subject", default=DEFAULT_SUBJECT,
                   help="Subject del esquema en el registro (convencion {topic}-value)")
    p.add_argument("--run-id", default=None,
                   help="Ejecucion del job a analizar (por defecto, la ultima)")
    p.add_argument("--grafana-url", default="http://localhost:3000")
    p.add_argument("--grafana-user", default="admin")
    p.add_argument("--grafana-password", default="admin")
    p.add_argument("--rango", default="now-24h",
                   help="Ventana temporal con la que se consultan los paneles")
    p.add_argument("--sin-grafana", action="store_true",
                   help="Omite el Objetivo 4 si Grafana no esta levantado)")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("kpi_report")
    try:
        sys.exit(run(parse_args()))
    except Exception as exc:
        logger.error("%s", exc)
        sys.exit(1)
