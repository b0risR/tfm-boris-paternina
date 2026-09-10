"""
Configuracion unica del registro de actividad de todos los procesos del pipeline.

Cada proceso escribe a la vez por consola —para verlo mientras corre— y a un
fichero propio bajo `pipeline/logs/`, de modo que despues de una prueba quede el
log de lo que hizo cada proceso.

Uso:
    from common.logging_setup import configurar_logging
    logger = configurar_logging("simulator")
"""

import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

DIRECTORIO_LOGS = Path(__file__).resolve().parents[1] / "logs"

FORMATO = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

# 3 MB por fichero y 1 copia de respaldo
TAMANO_MAX_BYTES = 3 * 1024 * 1024
COPIAS_RESPALDO = 1


def configurar_logging(nombre: str, nivel: int = logging.INFO,
                       directorio: Path | None = None) -> logging.Logger:
    """Deja el logging del proceso listo y devuelve su logger.
    """
    destino = directorio or DIRECTORIO_LOGS
    destino.mkdir(parents=True, exist_ok=True)
    fichero = destino / f"{nombre}.log"

    raiz = logging.getLogger()
    raiz.setLevel(nivel)

    ya_configurado = any(getattr(h, "_tfm_handler", False) for h in raiz.handlers)
    if not ya_configurado:
        formateador = logging.Formatter(FORMATO)

        consola = logging.StreamHandler(sys.stderr)
        consola.setFormatter(formateador)
        consola._tfm_handler = True
        raiz.addHandler(consola)

        rotatorio = RotatingFileHandler(
            fichero, maxBytes=TAMANO_MAX_BYTES, backupCount=COPIAS_RESPALDO,
            encoding="utf-8",
        )
        rotatorio.setFormatter(formateador)
        rotatorio._tfm_handler = True
        raiz.addHandler(rotatorio)

    logger = logging.getLogger(nombre)
    _cabecera_de_arranque(logger, fichero)
    return logger


def _cabecera_de_arranque(logger: logging.Logger, fichero: Path) -> None:
    """Marca el inicio de una ejecucion con su orden completa y su PID.
    """
    logger.info("=" * 78)
    logger.info("ARRANQUE %s | pid=%d | log=%s",
                time.strftime("%Y-%m-%d %H:%M:%S"), os.getpid(), fichero)
    logger.info("ORDEN: %s", " ".join(sys.argv))
    logger.info("=" * 78)
