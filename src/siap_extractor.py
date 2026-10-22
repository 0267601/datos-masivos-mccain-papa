"""
Compatibilidad con el entregable 1.

Desde el entregable 2 el extractor vive en src/ingestion/siap_extractor.py.
Dejamos este modulo para que notebooks/entregable_1.ipynb siga corriendo sin
cambios: solo reexporta lo de la ubicacion nueva.
"""

from src.ingestion.siap_extractor import *  # noqa: F401,F403
