"""
Simulador del parque de medidores sobre MQTT.

Uso:
    python mqtt_simulator.py --acelerar 2000
    python mqtt_simulator.py --acelerar 500 --ultimas-semanas 6     # demostracion en vivo

"""

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import aiomqtt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.logging_setup import configurar_logging
from common.stop_event import evento_de_parada_async
from simulator_helper import anadir_argumentos_dataset, preparar

logger = logging.getLogger("mqtt_simulator")

TOPIC_TEMPLATE = "iot/{building_id}/{meter_type}/telemetry"

# Cadencia del medidor real, en segundos
PERIODO_SENSOR_S = 3600.0

# Mimetiza el retardo de transmision de un enlace NB-IoT.
RETARDO_TRANSMISION_MS = 20_000


def build_topic(fila) -> str:
    """Topico del sensor que emite la lectura.
    """
    return TOPIC_TEMPLATE.format(building_id=fila.building_id, meter_type=fila.meter_type)


def build_payload(fila) -> dict:
    """Mensaje MQTT
    """
    return {
        "building_id": str(fila.building_id),
        "meter_type": str(fila.meter_type),
        "timestamp": fila.timestamp.isoformat(),
        "meter_reading": float(fila.meter_reading),
        "sim_publish_ts": int(time.time() * 1000) - RETARDO_TRANSMISION_MS,
    }


def anadir_argumentos_mqtt(p: argparse.ArgumentParser, client_id: str) -> None:
    """Conexion al broker."""
    p.add_argument("--broker-host", default="localhost")
    p.add_argument("--broker-port", type=int, default=1883)
    p.add_argument("--client-id", default=client_id)
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2],
                   help="QoS MQTT (1 por defecto, ver Objetivo 1)")


class Estado:
    """Recuento compartido entre las corrutinas."""

    def __init__(self):
        self.publicados = 0
        self.t0 = time.monotonic()


def programa_por_sensor(df) -> list[tuple]:
    """Cada sensor con su desfase determinista dentro del intervalo, en [0, 1).

    Es lo que impide que los 652 medidores publiquen a la vez: equivale a suponer
    que sus relojes no estan sincronizados al milisegundo, que es lo que ocurre
    en un parque real y ademas evita la rafaga que desbordaria al bridge. El
    desfase es determinista (idx / n), no aleatorio, para que dos ejecuciones con
    los mismos argumentos sean comparables. No tiene que ver con `--acelerar`.

    Las lecturas de un mismo sensor salen SIEMPRE por su propia conexion, en
    orden.
    """
    sensores = list(df.groupby(["building_id", "meter_type"], sort=True))
    n = len(sensores)
    return [(filas, idx / n) for idx, (_clave, filas) in enumerate(sensores)]


def _programa(filas, fraccion: float, t_sim0, speedup: float):
    """Instantes de publicacion de UN sensor, en segundos desde el arranque.

    El instante sale del tiempo de evento dividido por `speedup` —el mismo factor
    para todos los sensores—, mas el desfase propio del sensor dentro de su
    intervalo.
    """
    desfase = fraccion * PERIODO_SENSOR_S / speedup
    for fila in filas.itertuples(index=False):
        yield (fila.timestamp - t_sim0).total_seconds() / speedup + desfase, fila


async def cliente_mqtt(indice: int, filas, fraccion: float, args, estado: Estado,
                       parada, t0: float, t_sim0) -> None:
    """Una conexion MQTT publicando las lecturas de UN sensor.

    RECONECTA Y REINTENTA LO NO CONFIRMADO, que es lo que hace un medidor real:
    los medidores inteligentes guardan el perfil de carga y lo vuelcan cuando
    recuperan el enlace.
    """
    identificador = f"{args.client_id}-{indice}"
    # La agenda se construye UNA vez y se conserva entre reconexiones: es un
    # iterador perezoso, asi que reanudar es seguir consumiendolo, no repetirlo.
    agenda = _programa(filas, fraccion, t_sim0, args.speedup)
    pendiente = None
    intentos = 0

    while not parada.is_set():
        try:
            async with aiomqtt.Client(args.broker_host, args.broker_port,
                                      identifier=identificador) as cliente:
                intentos = 0
                while not parada.is_set():
                    if pendiente is None:
                        pendiente = next(agenda, None)
                        if pendiente is None:
                            return
                    instante, fila = pendiente

                    espera = (t0 + instante) - time.monotonic()
                    if espera > 0:
                        try:
                            await asyncio.wait_for(parada.wait(), timeout=espera)
                            return
                        except asyncio.TimeoutError:
                            pass

                    await cliente.publish(build_topic(fila),
                                          payload=json.dumps(build_payload(fila)),
                                          qos=args.qos)
                    estado.publicados += 1
                    pendiente = None

        except aiomqtt.MqttError as exc:
            intentos += 1
            if intentos > args.max_reconexiones:
                logger.error("[%s] sin conexion tras %d intentos, se abandona este sensor: %s",
                             identificador, intentos, exc)
                return
            logger.warning("[%s] conexion perdida (intento %d de %d), reintento en %.0f s: %s",
                           identificador, intentos, args.max_reconexiones,
                           args.espera_reconexion, exc)
            try:
                await asyncio.wait_for(parada.wait(), timeout=args.espera_reconexion)
                return
            except asyncio.TimeoutError:
                pass


async def simular(args, df) -> Estado:
    parada = await evento_de_parada_async("simulador")
    trabajo = programa_por_sensor(df)
    n_sensores = len(trabajo)

    tasa_teorica = n_sensores * args.speedup / PERIODO_SENSOR_S
    logger.info("Parque simulado: %d sensores | speedup x%s", n_sensores, f"{args.speedup:,.0f}")
    logger.info("Cadencia por sensor: %.3f s | tasa agregada teorica: %.1f ev/s",
                PERIODO_SENSOR_S / args.speedup, tasa_teorica)

    estado = Estado()
    t_sim0 = df["timestamp"].min()
    t0 = time.monotonic()

    tareas = [asyncio.create_task(
                  cliente_mqtt(i, filas, fraccion, args, estado, parada, t0, t_sim0))
              for i, (filas, fraccion) in enumerate(trabajo)]
    await asyncio.gather(*tareas)
    return estado


def run(args: argparse.Namespace) -> int:
    df = preparar(args.telemetry, args.limit, args.ultimas_semanas)
    estado = asyncio.run(simular(args, df))
    duracion = time.monotonic() - estado.t0

    logger.info("--- Fin de la simulacion: %s eventos publicados en %.0f s ---",
                f"{estado.publicados:,}", duracion)
    if estado.publicados < len(df):
        logger.warning("Agenda incompleta: %d de %d eventos (conexiones abandonadas o "
                       "parada anticipada)", estado.publicados, len(df))
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Simulador del parque de medidores IoT")
    anadir_argumentos_dataset(p)
    anadir_argumentos_mqtt(p, client_id="tfm-sim")
    p.add_argument("--acelerar", dest="speedup", metavar="FACTOR", type=float, default=2000.0,
                   help="Reproduce el historico FACTOR veces mas rapido, igual para todos los "
                        "sensores. Tasa agregada resultante = n_sensores x FACTOR / 3600 "
                        "(con 652 sensores y FACTOR=2000, unos 362 ev/s)")
    p.add_argument("--max-reconexiones", type=int, default=20,
                   help="Intentos de reconexion por sensor antes de abandonarlo. Un medidor "
                        "real reintenta y vuelca lo acumulado cuando recupera el enlace")
    p.add_argument("--espera-reconexion", type=float, default=3.0,
                   help="Segundos entre intentos de reconexion")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("mqtt_simulator")
    try:
        raise SystemExit(run(parse_args()))
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
    except aiomqtt.MqttError as exc:
        logger.error("No se pudo hablar con el broker MQTT: %s. Levanta el stack: "
                     "docker compose -f pipeline/docker-compose.yml up -d", exc)
        raise SystemExit(1)
