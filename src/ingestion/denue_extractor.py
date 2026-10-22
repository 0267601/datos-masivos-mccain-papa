"""
Extractor del DENUE (INEGI) — sector 72, nacional, varias versiones.

Obtenemos esta fuente por descarga automatizada de los zip de la "Descarga
masiva" de INEGI. Cada version trae el directorio completo de negocios del
sector 72 (alojamiento y preparacion de alimentos): unos 700 mil
establecimientos por version y ~430 MB de CSV descomprimido.

Tres cosas que encontramos al revisar los zip y que manejamos aqui o en
src/processing/denue.py:

1. INEGI responde 200 con una pagina HTML cuando el archivo no existe (un
   "404 suave"). Si guardaramos esa respuesta, la cache creeria que ya
   descargamos. Por eso revisamos que el contenido empiece con la firma de un
   zip ("PK") antes de aceptarlo.
2. Desde 2019 el sector viene partido en dos archivos (_1 y _2).
3. El esquema cambia entre versiones (schema drift, otra vez):
       2015       -> encabezados largos ("Clave municipio"), UTF-8,
                     fechas como "JULIO 2010", CSV en la raiz del zip
       2019-2026  -> encabezados cortos ("cve_mun"), Latin-1, fechas
                     "2026-04", CSV en conjunto_de_datos/

Pipeline:
    URL -> requests (por bloques) -> zip -> data/raw/denue/<version>/
"""

import hashlib

import requests

FIRMA_ZIP = b"PK"
BLOQUE = 1024 * 1024        # escribimos de 1 MB en 1 MB: los zip pesan ~50 MB


def nombre_archivo(url):
    """El nombre del zip es lo ultimo de la URL."""
    return url.rstrip("/").rsplit("/", 1)[-1]


def descargar_zip(url, destino, timeout=900):
    """Descargamos un zip por bloques a <destino>.parcial y lo renombramos.

    Devolvemos (bytes, sha256). Si lo que llega no es un zip, borramos el
    temporal y lanzamos ValueError: es el 404 suave de INEGI.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(destino.suffix + ".parcial")
    huella = hashlib.sha256()
    total = 0

    with requests.get(url, stream=True, timeout=timeout) as respuesta:
        respuesta.raise_for_status()
        with open(temporal, "wb") as archivo:
            for bloque in respuesta.iter_content(BLOQUE):
                if total == 0 and not bloque.startswith(FIRMA_ZIP):
                    archivo.close()
                    temporal.unlink(missing_ok=True)
                    raise ValueError(f"{nombre_archivo(url)} no es un zip "
                                     "(INEGI regreso una pagina HTML)")
                archivo.write(bloque)
                huella.update(bloque)
                total += len(bloque)

    try:
        temporal.replace(destino)
    except OSError:
        # Un Volume de Databricks no deja renombrar encima de un archivo.
        destino.write_bytes(temporal.read_bytes())
        temporal.unlink(missing_ok=True)

    return total, huella.hexdigest()
