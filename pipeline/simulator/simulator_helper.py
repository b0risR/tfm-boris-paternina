"""
Interpretacion del dataset

Este modulo decide QUE filas se reproducen y con que marcas de tiempo

"""

import argparse
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RUTA_TELEMETRIA = Path(__file__).resolve().parents[1] / "data" / "ashrae_telemetry.parquet"


# --------------------------------------------------------------------------
# Seleccion del subconjunto a publicar
# --------------------------------------------------------------------------
def cargar(telemetry_path: Path) -> pd.DataFrame:
    """Carga la tabla de hechos.
    """
    if not telemetry_path.exists():
        raise FileNotFoundError(
            f"No se encontro {telemetry_path}. Genera los datos primero: "
            "python pipeline/data/prepare_ashrae.py"
        )

    df = pd.read_parquet(telemetry_path)
    logger.info("Cargados %s eventos | %s sensores | %s edificios",
                f"{len(df):,}",
                f"{df.groupby(['building_id', 'meter_type']).ngroups:,}",
                f"{df['building_id'].nunique():,}")
    return df


def filtrar_ultimas_semanas(df: pd.DataFrame, semanas: int) -> pd.DataFrame:
    """Se queda con las ultimas `semanas` de datos, medidas desde la fecha mas
    reciente del conjunto.
    """
    corte = df["timestamp"].max() - pd.Timedelta(weeks=semanas)
    recorte = df[df["timestamp"] >= corte].reset_index(drop=True)
    logger.info("Ultimas %d semanas: %s -> %s (%s eventos de %s)", semanas,
                recorte["timestamp"].min(), recorte["timestamp"].max(),
                f"{len(recorte):,}", f"{len(df):,}")
    return recorte


def preparar(telemetry_path: Path | None = None,
             limite: int | None = None, ultimas_semanas: int | None = None,
             df: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    1. Ordenar cronologicamente.
    2. Recortar la ventana DESPUES de ordenar.
    """
    if df is None:
        df = cargar(telemetry_path)
    df = df.sort_values("timestamp").reset_index(drop=True)
    if ultimas_semanas:
        df = filtrar_ultimas_semanas(df, ultimas_semanas)
    if limite:
        df = df.head(limite)
    return df


def anadir_argumentos_dataset(p: argparse.ArgumentParser) -> None:
    """Opciones de seleccion del subconjunto."""
    p.add_argument("--telemetry", type=Path, default=RUTA_TELEMETRIA,
                   help="Tabla de hechos generada por prepare_ashrae.py")
    p.add_argument("--limite", dest="limit", metavar="N", type=int, default=None,
                   help="Numero maximo de eventos a publicar")
    p.add_argument("--ultimas-semanas", dest="ultimas_semanas", metavar="N", type=int, default=None,
                   help="Publica solo las ultimas N semanas del historico")
