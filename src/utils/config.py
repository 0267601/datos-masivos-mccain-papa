"""
Lectura de la configuracion del pipeline.

Leemos config/config.yaml si existe (la copia local de cada quien, que no se
sube) y, si no, config/config.example.yaml (la que si esta en el repositorio).
Asi el pipeline corre desde una instalacion limpia sin pasos manuales, y cada
quien puede cambiar anios o pausas sin tocar el codigo.
"""

import yaml

from src.utils.rutas import CONFIG


def cargar_config(ruta=None):
    """Devolvemos la configuracion como diccionario."""
    if ruta is None:
        local = CONFIG / "config.yaml"
        ruta = local if local.exists() else CONFIG / "config.example.yaml"

    with open(ruta, encoding="utf-8") as archivo:
        config = yaml.safe_load(archivo)

    # Los ProductoId llegan como texto si alguien los escribe entre comillas.
    config["sniim"]["variedades"] = {
        int(k): v for k, v in config["sniim"]["variedades"].items()
    }

    return config
