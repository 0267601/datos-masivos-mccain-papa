"""
Registro (logging) del pipeline.

Escribimos en consola y en logs/pipeline.log. El archivo nos sirve de
trazabilidad: si una descarga falla a media corrida, queda registrado que
particion fallo y por que, sin depender de lo que alcanzamos a ver en pantalla.
"""

import logging

from src.utils.rutas import LOGS

FORMATO = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def configurar_registro(nombre, nivel=logging.INFO):
    """Devolvemos un logger que escribe en consola y en logs/pipeline.log."""
    LOGS.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(nombre)
    if logger.handlers:            # evitamos duplicar handlers si se llama dos veces
        return logger

    logger.setLevel(nivel)
    formato = logging.Formatter(FORMATO, datefmt="%Y-%m-%d %H:%M:%S")

    consola = logging.StreamHandler()
    consola.setFormatter(formato)
    logger.addHandler(consola)

    archivo = logging.FileHandler(LOGS / "pipeline.log", encoding="utf-8")
    archivo.setFormatter(formato)
    logger.addHandler(archivo)

    logger.propagate = False
    return logger
