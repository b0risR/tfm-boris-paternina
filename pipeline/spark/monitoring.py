"""
Progreso de micro-lote y supervision de las consultas.

  1. Registrar cuanto tarda cada micro-lote en `TimescaleDB.streaming_progress`

  2. Comprobar que las dos consultas de streaming siguen vivas y relanzar desde
     su checkpoint la que caiga.

"""

import logging
from datetime import datetime
import time as _time

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Lectura del progreso
# --------------------------------------------------------------------------
def _a_timestamp(valor) -> datetime | None:
    """Convierte a datetime las marcas ISO que Spark publica como cadena."""
    if not valor:
        return None
    try:
        return datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    except ValueError:
        return None


def _descartados_por_watermark(progreso: dict) -> int | None:
    """Eventos que Spark tiro por llegar tarde, sumando todos los operadores.
    """
    operadores = progreso.get("stateOperators") or []
    valores = [o.get("numRowsDroppedByWatermark") for o in operadores]
    presentes = [v for v in valores if v is not None]
    return sum(presentes) if presentes else None


def _offsets_pendientes(progreso: dict) -> int | None:
    """Cuanto tiene Kafka que la consulta todavia no ha leido.
    """
    for fuente in progreso.get("sources") or []:
        metricas = fuente.get("metrics") or {}
        valor = metricas.get("maxOffsetsBehindLatest")
        if valor is not None:
            try:
                return int(valor)
            except (TypeError, ValueError):
                return None
    return None


# --------------------------------------------------------------------------
# Registro en TimescaleDB
# --------------------------------------------------------------------------
class RegistroProgreso:
    """Vuelca a TimescaleDB el progreso de cada micro-lote.

    Se lee `recentProgress` que conserva los ultimos 100 informes, 
    asi que sondeando mas a menudo que cada 100 lotes no se pierde
    ninguno.

    Los ya escritos se llevan en un conjunto en memoria porque esos 100
    informes se solapan entre sondeos consecutivos.
    """

    def __init__(self, consultas, props: dict, run_id: str):
        self.consultas = list(consultas)
        self.props = props
        self.run_id = run_id
        self.vistos: set[tuple[str, int]] = set()

    def seguir(self, consultas) -> None:
        """Actualiza la lista de consultas vigiladas"""
        self.consultas = list(consultas)

    def volcar(self) -> int:
        import psycopg2
        from psycopg2.extras import execute_values

        filas = []
        for q in self.consultas:
            for p in q.recentProgress:
                clave = (q.name, p.get("batchId"))
                if clave in self.vistos:
                    continue
                self.vistos.add(clave)

                d = p.get("durationMs", {}) or {}
                et = p.get("eventTime", {}) or {}
                filas.append((
                    _a_timestamp(p.get("timestamp")), self.run_id, q.name, p.get("batchId"),
                    p.get("numInputRows"),
                    p.get("inputRowsPerSecond"), p.get("processedRowsPerSecond"),
                    d.get("triggerExecution"), d.get("addBatch"), d.get("queryPlanning"),
                    _a_timestamp(et.get("max")), _a_timestamp(et.get("watermark")),
                    _descartados_por_watermark(p), _offsets_pendientes(p),
                ))

        if not filas:
            return 0
 
        conn = None
        try:
            conn = psycopg2.connect(**self.props)
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO streaming_progress (
                        trigger_ts, run_id, query_name, batch_id,
                        num_input_rows, input_rows_per_second, processed_rows_per_second,
                        duration_ms, add_batch_ms, query_planning_ms,
                        event_time_max, watermark,
                        rows_dropped_by_watermark, offsets_behind
                    ) VALUES %s
                    ON CONFLICT (trigger_ts, run_id, query_name, batch_id) DO NOTHING
                """, filas, page_size=500)
        except Exception as exc:
            logger.warning("No se pudo registrar el progreso de %d lotes: %s", len(filas), exc)
            # Se reintentaran en el volcado siguiente: al no haberse escrito, se
            # sacan del conjunto de vistos.
            for fila in filas:
                self.vistos.discard((fila[2], fila[3]))
            return 0
        finally:
            if conn is not None:
                conn.close()
        return len(filas)

    def arrancar(self, intervalo: float) -> None:
        import threading

        def bucle():
            while True:
                _time.sleep(intervalo)
                escritos = self.volcar()
                if escritos:
                    logger.info("Progreso registrado: %d lotes nuevos (run_id=%s)",
                                escritos, self.run_id)

        threading.Thread(target=bucle, daemon=True).start()


# --------------------------------------------------------------------------
# Supervision de las consultas y relanzado
# --------------------------------------------------------------------------
def supervisar(arrancadores: dict, intervalo: float, max_reinicios: int,
               registro=None) -> int:
    """Vigila las consultas y relanza la que se caiga.
    """
    consultas = {nombre: arrancar() for nombre, arrancar in arrancadores.items()}
    reinicios = {nombre: 0 for nombre in arrancadores}
    if registro:
        registro.seguir(consultas.values())
    logger.info("Consultas en marcha: %s", list(consultas))

    while consultas:
        _time.sleep(intervalo)
        for nombre in list(consultas):
            consulta = consultas[nombre]
            if consulta.isActive:
                continue

            motivo = consulta.exception()
            logger.error("LA CONSULTA %s SE HA DETENIDO: %s", nombre,
                         str(motivo).strip().splitlines()[0] if motivo else "sin excepcion")

            if reinicios[nombre] >= max_reinicios:
                logger.error("Agotados los %d reinicios de %s; se abandona esa consulta",
                             max_reinicios, nombre)
                del consultas[nombre]
                continue

            reinicios[nombre] += 1
            logger.warning("Relanzando %s desde su checkpoint (reinicio %d de %d)...",
                           nombre, reinicios[nombre], max_reinicios)
            try:
                consultas[nombre] = arrancadores[nombre]()
                if registro:
                    registro.seguir(consultas.values())
            except Exception as exc:
                logger.error("No se pudo relanzar %s: %s", nombre, exc)
                del consultas[nombre]

    logger.error("No queda ninguna consulta activa; el job termina")
    return 1
