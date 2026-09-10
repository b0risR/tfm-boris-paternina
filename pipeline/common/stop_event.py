"""
Senal de parada para los procesos de larga duracion.
"""

import logging
import signal
import threading

logger = logging.getLogger(__name__)


def evento_de_parada(descripcion: str = "proceso") -> threading.Event:
    parada = threading.Event()

    def manejador(*_args):
        logger.info("Senal de parada recibida, cerrando %s...", descripcion)
        parada.set()

    signal.signal(signal.SIGINT, manejador)
    signal.signal(signal.SIGTERM, manejador)
    return parada


async def evento_de_parada_async(descripcion: str = "proceso") -> "asyncio.Event":
    import asyncio

    parada = asyncio.Event()
    bucle = asyncio.get_running_loop()

    def manejador():
        logger.info("Senal de parada recibida, cerrando %s...", descripcion)
        parada.set()

    for senal in (signal.SIGINT, signal.SIGTERM):
        bucle.add_signal_handler(senal, manejador)
    return parada
