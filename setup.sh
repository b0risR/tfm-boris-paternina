#!/usr/bin/env bash
#
# setup.sh — Prepara el entorno completo para ejecutar el pipeline: comprueba
# Docker y Java (no los instala: son herramientas de sistema, se guía pero no
# se automatiza), crea el entorno virtual e instala las dependencias Python,
# y descarga + prepara el dataset ASHRAE si hace falta.
#
# Portable: probado en Linux/WSL y macOS (Apple Silicon e Intel), sin
# sintaxis GNU-especifica.
#
# Uso:
#   bash setup.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
VENV_DIR="$REPO_ROOT/.venv"
PIPELINE_DIR="$REPO_ROOT/pipeline"
DATA_DIR="$PIPELINE_DIR/data"
RAW_DIR="$DATA_DIR/raw"

DOCKER_OK=1
JAVA_OK=1
DATASET_OK=1

echo "==================================================================="
echo "  setup.sh -- preparando el entorno del pipeline"
echo "==================================================================="

# ---------------------------------------------------------------------------
# 1/5 Docker: solo se comprueba y se guia
# ---------------------------------------------------------------------------
echo ""
echo "==> 1/5 Docker"
if ! command -v docker &> /dev/null; then
  DOCKER_OK=0
  echo "  FALTA: no se encontro 'docker' en el PATH."
  echo "  Instalalo desde https://docs.docker.com/get-docker/"
  echo "  (macOS: Docker Desktop. Linux/WSL: Docker Engine + el plugin compose)."
elif ! docker compose version &> /dev/null; then
  DOCKER_OK=0
  echo "  FALTA: 'docker' esta, pero 'docker compose' (v2) no responde."
  echo "  Actualiza Docker Desktop, o instala docker-compose-plugin en Linux."
else
  echo "  OK: $(docker --version)"
  echo "      $(docker compose version)"
fi

# ---------------------------------------------------------------------------
# 2/5 Java: solo se comprueba y se guia. PySpark exige 17+; cualquier
# distribucion sirve (Temurin, Corretto, Zulu, Homebrew...)
# ---------------------------------------------------------------------------
echo ""
echo "==> 2/5 Java (17+, recomendado 21 LTS)"
if ! command -v java &> /dev/null; then
  JAVA_OK=0
  echo "  FALTA: no se encontro 'java' en el PATH. Alternativas:"
  echo "    - SDKMAN:            curl -s \"https://get.sdkman.io\" | bash"
  echo "                         (luego: sdk env install && sdk env)"
  echo "    - Homebrew (macOS):  brew install openjdk@21"
  echo "    - Instalador grafico: https://adoptium.net (Temurin 21)"
else
  JAVA_VER_LINE="$(java -version 2>&1 | head -1)"
  JAVA_MAJOR="$(echo "$JAVA_VER_LINE" | sed -E 's/.*"([0-9]+)\.?[0-9]*.*/\1/')"
  # Esquema de versionado antiguo
  if [ "$JAVA_MAJOR" = "1" ]; then
    JAVA_MAJOR=8
  fi
  if ! [[ "$JAVA_MAJOR" =~ ^[0-9]+$ ]]; then
    JAVA_OK=0
    echo "  No se pudo interpretar la version: $JAVA_VER_LINE"
  elif [ "$JAVA_MAJOR" -ge 17 ]; then
    echo "  OK: $JAVA_VER_LINE"
  else
    JAVA_OK=0
    echo "  INSUFICIENTE: $JAVA_VER_LINE (se requiere 17+)."
  fi
fi

# ---------------------------------------------------------------------------
# 3/5 Entorno virtual: Si no existe, se comprueba antes que el modulo venv 
# de Python puede montar pip
# ---------------------------------------------------------------------------
echo ""
echo "==> 3/5 Entorno virtual ($VENV_DIR)"
if [ -x "$VENV_DIR/bin/python" ]; then
  echo "  Ya existe -- reutilizando."
elif ! python3 -c "import ensurepip" &> /dev/null; then
  echo "  FALTA: el modulo 'venv' de Python no puede crear entornos con pip."
  echo "  En Debian/Ubuntu (y WSL) suele faltar el paquete python3-venv:"
  echo "    sudo apt install python3-venv"
  echo "  Vuelve a ejecutar 'bash setup.sh' despues de instalarlo."
  exit 1
else
  echo "  Creando..."
  python3 -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# ---------------------------------------------------------------------------
# 4/5 Dependencias Python: requirements.txt
# ---------------------------------------------------------------------------
echo ""
echo "==> 4/5 Dependencias Python"
pip install --upgrade pip -q
pip install -r "$REPO_ROOT/requirements.txt"

# ---------------------------------------------------------------------------
# 5/5 Dataset ASHRAE: se salta si ya esta preparado
# ---------------------------------------------------------------------------
echo ""
echo "==> 5/5 Dataset ASHRAE"

TELEMETRIA="$DATA_DIR/ashrae_telemetry.parquet"
EDIFICIOS="$DATA_DIR/ashrae_buildings.parquet"
LINEA_BASE="$DATA_DIR/ashrae_sensor_baseline.parquet"

descargar_si_falta() {
  # $1: nombre del fichero en la competicion (p.ej. train.csv)
  local nombre="$1"
  local destino="$RAW_DIR/$nombre"
  if [ -f "$destino" ]; then
    return 0
  fi
  echo "  Descargando $nombre..."
  kaggle competitions download -c ashrae-energy-prediction -f "$nombre" -p "$RAW_DIR"
  # La API de competiciones no tiene --unzip (a diferencia de "datasets
  # download"): el fichero puede llegar comprimido sin avisar. Se comprueba y
  # se descomprime con el modulo zipfile de Python
  if [ -f "$destino.zip" ]; then
    python3 -c "import zipfile; zipfile.ZipFile('$destino.zip').extractall('$RAW_DIR')"
    rm -f "$destino.zip"
  fi
  if [ ! -f "$destino" ]; then
    echo "  ERROR: no se encontro $destino tras la descarga."
    return 1
  fi
}

if [ -f "$TELEMETRIA" ] && [ -f "$EDIFICIOS" ] && [ -f "$LINEA_BASE" ]; then
  echo "  Ya preparado -- los tres Parquet existen en $DATA_DIR"
else
  # La CLI de Kaggle prueba, en este orden: token de acceso (variable
  # KAGGLE_API_TOKEN o ~/.kaggle/access_token[.txt] 
  # y despues kaggle.json (metodo antiguo, usuario+clave).
  KAGGLE_TOKEN="$HOME/.kaggle/access_token"
  KAGGLE_TOKEN_TXT="$HOME/.kaggle/access_token.txt"
  KAGGLE_JSON="$HOME/.kaggle/kaggle.json"
  if [ ! -f "$KAGGLE_TOKEN" ] && [ ! -f "$KAGGLE_TOKEN_TXT" ] \
     && [ ! -f "$KAGGLE_JSON" ] && [ -z "${KAGGLE_API_TOKEN:-}" ]; then
    DATASET_OK=0
    echo "  PENDIENTE: no se encontraron credenciales de Kaggle."
    echo "  Requiere pasar por el navegador:"
    echo ""
    echo "    1. Entra en https://www.kaggle.com/settings/api y pulsa 'Create New Token'"
    echo "       -- copia el token que te muestra (empieza por 'KGAT_')."
    echo "       Solo se ve una vez: si lo pierdes, genera uno nuevo."
    echo ""
    echo "    2. Guardalo y protegelo (ejecuta esto en una terminal, sustituyendo"
    echo "       TU_TOKEN por el valor copiado):"
    echo "         mkdir -p $HOME/.kaggle"
    echo "         echo TU_TOKEN > $KAGGLE_TOKEN"
    echo "         chmod 600 $KAGGLE_TOKEN"
    echo "       (el permiso 600 es obligatorio: la CLI de Kaggle rechaza el"
    echo "       fichero si otros usuarios pueden leerlo)"
    echo ""
    echo "    3. Acepta las reglas de la competicion, tambien desde el navegador"
    echo "       (sin esto la descarga falla con error 403 aunque las"
    echo "       credenciales sean correctas):"
    echo "         https://www.kaggle.com/competitions/ashrae-energy-prediction/rules"
    echo ""
    echo "  Repite 'bash setup.sh' cuando hayas hecho los tres pasos."
  else
    mkdir -p "$RAW_DIR"
    if descargar_si_falta "train.csv" && descargar_si_falta "building_metadata.csv"; then
      echo "  Generando los tres Parquet (prepare_ashrae.py)..."
      # --fecha-final = hoy: reubica la serie de 2016 al presente para que la demo
      # en vivo tenga el ano actual.
      python "$DATA_DIR/prepare_ashrae.py" \
        --train "$RAW_DIR/train.csv" \
        --metadata "$RAW_DIR/building_metadata.csv" \
        --fecha-final "$(date +%F)"
    else
      DATASET_OK=0
      echo "  PENDIENTE: la descarga fallo. Motivo habitual: no se aceptaron"
      echo "  las reglas de la competicion en la pagina de Kaggle (enlace arriba)."
    fi
  fi
fi

# ---------------------------------------------------------------------------
# Resumen
# ---------------------------------------------------------------------------
echo ""
echo "==================================================================="
echo "  Resumen"
echo "==================================================================="
[ "$DOCKER_OK" -eq 1 ]  && echo "  [OK]        Docker"       || echo "  [PENDIENTE] Docker -- ver 1/5 mas arriba"
[ "$JAVA_OK" -eq 1 ]    && echo "  [OK]        Java"         || echo "  [PENDIENTE] Java -- ver 2/5 mas arriba"
echo "  [OK]        Entorno virtual y dependencias Python"
[ "$DATASET_OK" -eq 1 ] && echo "  [OK]        Dataset ASHRAE" || echo "  [PENDIENTE] Dataset ASHRAE -- ver 5/5 mas arriba"
echo ""

if [ "$DOCKER_OK" -eq 1 ] && [ "$JAVA_OK" -eq 1 ] && [ "$DATASET_OK" -eq 1 ]; then
  echo "  Todo listo. Para activar el entorno y levantar el pipeline:"
  echo "    source .venv/bin/activate"
  echo "    docker compose -f pipeline/docker-compose.yml up -d"
  echo "    python pipeline/tools/demo.py --semanas 6"
else
  echo "  Resuelve lo marcado como PENDIENTE y vuelve a ejecutar 'bash setup.sh'."
fi
echo "==================================================================="
