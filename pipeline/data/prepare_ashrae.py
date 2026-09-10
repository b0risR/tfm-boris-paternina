"""
Preparacion desde el dataset ASHRAE GEPIII

Une las lecturas de medidor con los metadatos de edificio, se queda con el
subconjunto de emplazamientos elegido y produce las tres tablas que
alimentan el pipeline.

Es un paso de UN SOLO USO. Funciona
con los ficheros originales de Kaggle (CSV) o con los mismos datos exportados a
Parquet; el formato se detecta por la extension.

    train + building_metadata  ->  ashrae_telemetry.parquet
                                   ashrae_buildings.parquet
                                   ashrae_sensor_baseline.parquet

SUBCONJUNTO ELEGIDO: emplazamientos 2, 3 y 5.

Se producen TRES ficheros, con nombres fijos, en este mismo directorio:

  - `ashrae_telemetry.parquet`       la tabla de hechos, lo que emite el medidor
  - `ashrae_buildings.parquet`       la dimension con los atributos del edificio
  - `ashrae_sensor_baseline.parquet` los cuartiles del historico de cada medidor,
                                     que usa Spark para detectar picos atipicos y
                                     Power BI para ajustar el umbral

Los nombres NO son configurables: el simulador y el job de
Spark ya los tienen guardados como valores por defecto.

REUBICACION TEMPORAL: los datos de ASHRAE son de 2016. Con `--fecha-final` se
desplaza toda la serie para que la ULTIMA lectura caiga en la fecha indicada.

Uso:
    python prepare_ashrae.py --fecha-final AAAA-MM-DD

"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.logging_setup import configurar_logging

logger = logging.getLogger("prepare_ashrae")

# Codigos de medidor segun la documentacion de Kaggle
METER_TYPES = {0: "electricity", 1: "chilledwater", 2: "steam", 3: "hotwater"}

# Emplazamientos del subconjunto
DEFAULT_SITES = (2, 3, 5)

AQUI = Path(__file__).parent
SALIDA_TELEMETRIA = AQUI / "ashrae_telemetry.parquet"
SALIDA_EDIFICIOS = AQUI / "ashrae_buildings.parquet"
SALIDA_LINEA_BASE = AQUI / "ashrae_sensor_baseline.parquet"


def read_any(path: Path) -> pd.DataFrame:
    """Lee CSV o Parquet segun la extension del fichero."""
    if not path.exists():
        raise FileNotFoundError(f"No se encontro {path}")
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def desplazar_a_fecha_final(tel: pd.DataFrame, fecha_final: str) -> pd.DataFrame:
    """Reubica la serie para que la ULTIMA lectura caiga en `fecha_final`.
    """
    destino = pd.Timestamp(datetime.fromisoformat(fecha_final))
    offset = destino - tel["timestamp"].max()
    tel = tel.copy()
    tel["timestamp"] = tel["timestamp"] + offset
    logger.info("Reubicacion temporal: marcas desplazadas %+.1f dias -> la ultima cae "
                "en %s (rango %s -> %s)", offset.total_seconds() / 86400, destino,
                tel["timestamp"].min(), tel["timestamp"].max())
    return tel


def prepare(train_path: Path, meta_path: Path, sites: tuple,
            fecha_final: str | None = None) -> None:
    logger.info(f"Leyendo metadatos de edificio: {meta_path}")
    meta = read_any(meta_path)
    meta_sub = meta[meta["site_id"].isin(sites)]
    logger.info(f"  {len(meta)} edificios en total -> {len(meta_sub)} en los emplazamientos {list(sites)}")

    logger.info(f"Leyendo lecturas de medidor: {train_path}")
    train = read_any(train_path)
    logger.info(f"  {len(train):,} lecturas en total")

    # El join hace tambien de filtro: solo quedan las lecturas de edificios
    # de los emplazamientos elegidos.
    df = train.merge(meta_sub, on="building_id", how="inner")
    logger.info(f"  {len(df):,} lecturas tras filtrar por emplazamiento")

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["meter_type"] = df["meter"].map(METER_TYPES)
    if df["meter_type"].isna().any():
        codigos = sorted(df.loc[df["meter_type"].isna(), "meter"].unique())
        raise ValueError(f"Codigos de medidor desconocidos en los datos: {codigos}")

    # ---- TABLA DE HECHOS: lo que emite el medidor -------------------
    telemetria = df[["building_id", "meter_type", "timestamp", "meter_reading"]].copy()
    telemetria["building_id"] = telemetria["building_id"].astype(str)
    telemetria = telemetria.sort_values("timestamp").reset_index(drop=True)

    # Reubicacion temporal opcional: deja el Parquet datado en AAAA-MM-DD
    if fecha_final:
        telemetria = desplazar_a_fecha_final(telemetria, fecha_final)
    else:
        logger.warning("Sin --fecha-final: los datos quedan en 2016. La demo en vivo "
                       "saldria vacia (los paneles miran a fechas recientes). Pasa una "
                       "fecha reciente, idealmente hoy, para reubicar la serie.")

    # ---- TABLA DE DIMENSION: caracteristicas estaticas del edificio ----------
    dimension = (
        meta_sub[["building_id", "site_id", "primary_use",
                  "square_feet", "year_built", "floor_count"]]
        .sort_values("building_id").reset_index(drop=True)
    )
    dimension["building_id"] = dimension["building_id"].astype(str)

    # ---- LINEA BASE POR SENSOR: referencia para la deteccion de picos --------
    # Mediana y cuartiles del historico de cada medidor. Se marca como atipica 
    # toda lectura que supere p75 + 5*IQR de su PROPIO sensor.
    g = telemetria.groupby(["building_id", "meter_type"], observed=True)["meter_reading"]
    linea_base = g.agg(
        baseline_p25=lambda s: s.quantile(0.25),
        baseline_p50="median",
        baseline_p75=lambda s: s.quantile(0.75),
    ).reset_index()
    linea_base["baseline_iqr"] = linea_base.baseline_p75 - linea_base.baseline_p25

    resumen(telemetria, dimension, linea_base)

    logger.info("")
    for datos, ruta in ((telemetria, SALIDA_TELEMETRIA),
                        (dimension, SALIDA_EDIFICIOS),
                        (linea_base, SALIDA_LINEA_BASE)):
        ruta.parent.mkdir(parents=True, exist_ok=True)
        datos.to_parquet(ruta, index=False)
        logger.info(f"Escrito {ruta} ({ruta.stat().st_size / 1024:.0f} KB)")


def resumen(tel: pd.DataFrame, dim: pd.DataFrame, base: pd.DataFrame) -> None:
    n = len(tel)
    logger.info("\n--- Tabla de hechos ---")
    logger.info(f"  columnas               : {list(tel.columns)}")
    logger.info(f"  eventos                : {n:,}")
    logger.info(f"  sensores (edificio + medidor): {tel.groupby(['building_id','meter_type'], observed=True).ngroups}")
    logger.info(f"  rango temporal         : {tel['timestamp'].min()} -> {tel['timestamp'].max()}")
    logger.info(f"  nulos                  : {int(tel.isna().sum().sum())}")

    clave = ["building_id", "meter_type", "timestamp"]
    grupos = tel.groupby(clave, observed=True).ngroups
    logger.info(f"\n  clave natural {tuple(clave)}:")
    logger.info(f"    grupos = {grupos:,} sobre {n:,} filas -> {'UNICA' if grupos == n else 'COLISIONA'}")

    sin_contador = tel.groupby(["building_id", "timestamp"], observed=True).ngroups
    logger.info(f"    sin meter_type seria {sin_contador:,} grupos -> perderia {n - sin_contador:,} eventos")

    ceros = (tel["meter_reading"] == 0).sum()
    logger.info(f"\n  lecturas a cero        : {ceros:,} ({100*ceros/n:.1f}%)")
    logger.info("  (no se eliminan: son dato real y material para el informe de anomalias)")

    logger.info("\n  eventos por tipo de medidor:")
    for tipo, c in tel["meter_type"].value_counts().items():
        logger.info(f"    {tipo:<14} {c:>10,}")

    logger.info("\n--- Tabla de dimension (atributos del edificio) ---")
    logger.info(f"  columnas               : {list(dim.columns)}")
    logger.info(f"  edificios              : {len(dim):,}")
    logger.info(f"  emplazamientos         : {sorted(int(s) for s in dim['site_id'].unique())}")
    logger.info(f"  usos de edificio       : {dim['primary_use'].nunique()}")
    logger.info("  nulos por columna:")
    nulos = dim.isna().sum()
    for col, c in nulos[nulos > 0].items():
        logger.info(f"    {col:<16} {c:>5,} de {len(dim)}  ({100*c/len(dim):.1f}%)")

    logger.info("\n--- Linea base por sensor (referencia de deteccion de picos) ---")
    logger.info(f"  sensores               : {len(base):,}")
    logger.info(f"  columnas               : {list(base.columns)}")
    sin_dispersion = (base.baseline_iqr == 0).sum()
    logger.info(f"  sensores con IQR = 0   : {sin_dispersion} (quedan exentos de la regla de picos)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prepara el subconjunto ASHRAE para el pipeline (TFM)")
    p.add_argument("--train", type=Path, default=AQUI / "raw/train.parquet",
                   help="Lecturas de medidor (train.csv de Kaggle o su equivalente en Parquet)")
    p.add_argument("--metadata", type=Path, default=AQUI / "raw/building_metadata.parquet",
                   help="Metadatos de edificio (building_metadata.csv o Parquet)")
    p.add_argument("--sites", type=int, nargs="+", default=list(DEFAULT_SITES),
                   help="Emplazamientos a incluir (por defecto 2 3 5)")
    p.add_argument("--fecha-final", dest="fecha_final", default=None, metavar="AAAA-MM-DD",
                   help="Reubica la serie para que la ULTIMA lectura caiga en esta fecha ")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("prepare_ashrae")
    a = parse_args()
    prepare(a.train, a.metadata, tuple(a.sites), a.fecha_final)
