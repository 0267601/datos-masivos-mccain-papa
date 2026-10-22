"""
Rutas del proyecto.

Resolvemos todas las rutas a partir de la ubicacion del repositorio, igual que
en el notebook del entregable 1. Asi el pipeline corre igual en la computadora
de cualquiera de nosotros, en la del profesor y en Colab, sin rutas personales.

Capas de datos que seguimos:
    raw/        -> datos tal como los entrega la fuente (sin modificar)
    interim/    -> datos parseados y con esquema unificado, aun como texto
    processed/  -> datos tipados, limpios y listos para analisis

Donde viven las capas:
    Local       -> data/ dentro del repositorio (no se sube a GitHub)
    Databricks  -> un Volume de Unity Catalog, el data lake del proyecto:
                   /Volumes/workspace/mccain_papa/datalake/{raw,interim,processed}

Para cambiar de lugar las capas sin tocar codigo usamos variables de entorno,
que hay que definir ANTES de importar este modulo:
    DATOS_MASIVOS_DATA     -> raiz de las capas raw/interim/processed
    DATOS_MASIVOS_REPORTS  -> carpeta de tablas y figuras de resultados
    DATOS_MASIVOS_LOGS     -> carpeta del registro del pipeline
"""

import os
from pathlib import Path

# src/utils/rutas.py -> subimos dos niveles para llegar a la raiz del repo.
RAIZ = Path(__file__).resolve().parents[2]

# Databricks define esta variable en todos sus entornos de ejecucion.
EN_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# Las copias del E1 y las muestras siempre viven en el repositorio.
DATA_REPO = RAIZ / "data"
SAMPLE = DATA_REPO / "sample"

DATA = Path(os.environ.get("DATOS_MASIVOS_DATA", DATA_REPO))
RAW = DATA / "raw"
INTERIM = DATA / "interim"
PROCESSED = DATA / "processed"

CONFIG = RAIZ / "config"
REPORTS = Path(os.environ.get("DATOS_MASIVOS_REPORTS", RAIZ / "reports"))
TABLAS = REPORTS / "tables"
FIGURAS = REPORTS / "figures"

# En Databricks escribimos el registro en el disco local del driver: los
# archivos de un Volume y de un Git folder no aceptan que les agreguemos
# lineas al final (append), que es justo lo que hace el registro.
LOGS = Path(os.environ.get(
    "DATOS_MASIVOS_LOGS",
    "/tmp/datos_masivos_logs" if EN_DATABRICKS else RAIZ / "logs",
))


def asegurar_carpetas():
    """Creamos las carpetas de trabajo si todavia no existen."""
    for carpeta in [RAW, INTERIM, PROCESSED, LOGS, TABLAS, FIGURAS]:
        carpeta.mkdir(parents=True, exist_ok=True)
