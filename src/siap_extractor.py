"""
Extractor de produccion y valor de venta de papa — SIAP (SADER)

Obtenemos esta fuente mediante descarga automatizada de los CSV del
Cierre de la Produccion Agricola, a nivel municipal, de 2003 a 2025.

Tres problemas de calidad que encontramos y corregimos aqui:

1. Los encabezados cambian entre anios (schema drift):
       2013 y previos   -> "Nomcultivo"        / "Precio"
       2016-2019        -> "Nomcultivo Sin Um" / "Precio"
       2021 en adelante -> "Nomcultivo"        / "Preciomediorural"
   Si concatenamos sin normalizar, el codigo truena.

2. El archivo viene en latin-1, no en utf-8.

3. Las columnas numericas llegan como texto en algunos anios. Pasa porque
   otros cultivos del mismo CSV traen valores no numericos y pandas infiere
   "object" para toda la columna; al filtrar papa el tipo se hereda.

Pipeline que seguimos:
    URL -> CSV -> pandas -> normalizar -> DataFrame -> CSV
"""

from io import BytesIO
import time

import pandas as pd
import requests

URL_DESCARGA = "https://nube.agricultura.gob.mx/index.php"
VISTA_MUNICIPAL = "10AE434F-A2158368-A120BC5A-EDF4AFAA"

# Unificamos los encabezados que cambiaron de nombre entre anios.
RENOMBRES = {
    "Nomcultivo Sin Um": "Nomcultivo",
    "Precio": "Preciomediorural",
}

COLUMNAS_NUMERICAS = [
    "Sembrada", "Cosechada", "Siniestrada",
    "Volumenproduccion", "Rendimiento", "Preciomediorural", "Valorproduccion",
]

# Conservamos Nomddr y Nomcader a proposito: SIAP desagrega por debajo del
# municipio, asi que sin ellas la llave no es unica y nos aparecen
# 186 falsos duplicados en el diagnostico.
COLUMNAS_UTILES = [
    "Anio", "Nomestado", "Nomddr", "Nomcader", "Nommunicipio",
    "Nomcicloproductivo", "Nommodalidad",
    "Nomcultivo", "Sembrada", "Cosechada", "Siniestrada",
    "Volumenproduccion", "Rendimiento", "Preciomediorural", "Valorproduccion",
]


def descargar_anio(anio, cultivo="papa", timeout=180):
    """Descargamos un anio del cierre agricola y normalizamos su esquema."""
    params = {"view": VISTA_MUNICIPAL, "ANIO": anio}

    respuesta = requests.get(URL_DESCARGA, params=params, timeout=timeout)
    respuesta.raise_for_status()

    # Declaramos latin-1 explicitamente: el portal no lo anuncia en la cabecera.
    df = pd.read_csv(BytesIO(respuesta.content), encoding="latin-1", low_memory=False)

    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=RENOMBRES)

    if cultivo:
        df = df[df["Nomcultivo"].astype(str).str.strip().str.lower() == cultivo.lower()]

    df = df[[c for c in COLUMNAS_UTILES if c in df.columns]].copy()

    # Forzamos los tipos numericos despues de filtrar, porque el dtype que
    # heredamos del CSV completo puede ser texto.
    for columna in COLUMNAS_NUMERICAS:
        if columna in df.columns:
            df[columna] = pd.to_numeric(
                df[columna].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )

    df["Anio"] = df["Anio"].astype(int)

    return df.reset_index(drop=True), len(respuesta.content) / 1024**2


def descargar_anios(anios, cultivo="papa", pausa=0.5, verbose=True):
    """Descargamos varios anios, uno a la vez, y reportamos metricas.

    Descargamos el CSV completo de cada anio (unos 6 MB) y nos quedamos solo
    con papa. Por eso reportamos los MB descargados aparte de los MB que
    terminan en memoria: la diferencia es grande y vale la pena verla.
    """
    partes = []
    metricas = []

    for anio in anios:
        df_anio, mb = descargar_anio(anio, cultivo)

        partes.append(df_anio)
        metricas.append({
            "anio": anio,
            "filas_papa": len(df_anio),
            "descarga_mb": mb,
            "ram_mb": df_anio.memory_usage(deep=True).sum() / 1024**2,
        })

        if verbose:
            print(f"{anio} | {len(df_anio):4,} filas de papa | {mb:5.1f} MB descargados")

        time.sleep(pausa)

    df = pd.concat(partes, ignore_index=True)

    return df, pd.DataFrame(metricas)
