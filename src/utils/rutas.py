"""
Rutas del proyecto.

Resolvemos todas las rutas a partir de la ubicacion del repositorio, igual que
en el notebook del entregable 1. Asi el pipeline corre igual en la computadora
de cualquiera de nosotros, en la del profesor y en Colab, sin rutas personales.

Capas de datos que seguimos:
    data/raw/        -> datos tal como los entrega la fuente (sin modificar)
    data/interim/    -> datos parseados y con esquema unificado, aun como texto
    data/processed/  -> datos tipados, limpios y listos para analisis
    data/sample/     -> muestra pequena que si subimos al repositorio
"""

from pathlib import Path

# src/utils/rutas.py -> subimos dos niveles para llegar a la raiz del repo.
RAIZ = Path(__file__).resolve().parents[2]

DATA = RAIZ / "data"
RAW = DATA / "raw"
INTERIM = DATA / "interim"
PROCESSED = DATA / "processed"
SAMPLE = DATA / "sample"

CONFIG = RAIZ / "config"
LOGS = RAIZ / "logs"
REPORTS = RAIZ / "reports"
TABLAS = REPORTS / "tables"
FIGURAS = REPORTS / "figures"


def asegurar_carpetas():
    """Creamos las carpetas de trabajo si todavia no existen."""
    for carpeta in [RAW, INTERIM, PROCESSED, LOGS, TABLAS, FIGURAS]:
        carpeta.mkdir(parents=True, exist_ok=True)
