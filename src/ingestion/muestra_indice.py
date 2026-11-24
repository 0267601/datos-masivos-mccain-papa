"""
Muestra real del indice de inversion: un extracto chico del DENUE y Comtrade.

El DENUE completo pesa ~225 MB en zip y no cabe en el repositorio. Para que el
pipeline del indice se pueda correr sin internet (y revisar en clase) dejamos
en data/sample/indice/ un extracto real:

    denue/<version>/*.zip   los establecimientos de unas pocas metropolis, en
                            el mismo formato que publica INEGI en cada version:
                            2015 con encabezados largos y UTF-8, CSV en la raiz;
                            2019 en adelante con encabezados cortos, Latin-1 y
                            el CSV dentro de conjunto_de_datos/
    comtrade/<YYYYMM>.json  los 132 meses, solo con los campos que usamos

Lo genera Databricks (notebook 05) porque ahi vive la descarga completa:

    exportar_muestra_indice(spark, ["Los Cabos", ...], destino)

y download_data --muestra lo copia a data/raw/ (ver muestra.py).

Pipeline:
    interim/denue (Volume) -> filtro por metropoli -> zip por version -> sample/
    raw/comtrade (Volume)  -> campos usados        -> JSON por mes  -> sample/
"""

import io
import json
import zipfile

from pyspark.sql import functions as F

from src.processing.denue import COLUMNAS

# Encabezados de la version 2015 (los largos que traducimos en denue.ALIAS).
ENCABEZADOS_2015 = {
    "id": "ID", "nom_estab": "Nombre de la Unidad Económica", "raz_social": "Razón social",
    "codigo_act": "Código de la clase de actividad SCIAN",
    "nombre_act": "Nombre de clase de la actividad",
    "per_ocu": "Descripción estrato personal ocupado", "cve_ent": "Clave entidad",
    "entidad": "Entidad federativa", "cve_mun": "Clave municipio", "municipio": "Municipio",
    "latitud": "Latitud", "longitud": "Longitud",
    "fecha_alta": "Fecha de incorporación al DENUE",
}
CAMPOS_COMTRADE = ["period", "partnerCode", "partnerDesc", "primaryValue", "netWgt",
                   "customsCode", "motCode", "partner2Code"]


def zip_version(df, etiqueta):
    """Armamos el zip de una version con el formato original de INEGI."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archivo:
        if etiqueta.startswith("2015"):
            texto = df.rename(columns=ENCABEZADOS_2015).to_csv(index=False)
            archivo.writestr("DENUE_INEGI_72_.csv", ("﻿" + texto).encode("utf-8"))
        else:
            texto = df.to_csv(index=False)
            archivo.writestr("conjunto_de_datos/denue_inegi_72_.csv",
                             texto.encode("latin-1", errors="replace"))
    return buffer.getvalue()


def exportar_muestra_indice(spark, nombres_metropoli, interim_denue, municipios,
                            raw_comtrade, destino):
    """Escribimos la muestra en destino/denue y destino/comtrade. Regresamos conteos."""
    claves = (municipios.filter(F.col("metropoli").isin(nombres_metropoli))
              .select("cve_mun5").distinct())
    extracto = (
        spark.read.parquet(str(interim_denue))
        .withColumn("cve_mun5", F.concat(F.lpad(F.trim("cve_ent"), 2, "0"),
                                         F.lpad(F.trim("cve_mun"), 3, "0")))
        .join(F.broadcast(claves), "cve_mun5")
        .select("version", *COLUMNAS)
        .toPandas()
    )
    if extracto.empty:
        raise ValueError(f"Ninguna metropoli coincide con {nombres_metropoli}")

    conteo = {}
    for etiqueta, grupo in extracto.groupby("version"):
        carpeta = destino / "denue" / etiqueta
        carpeta.mkdir(parents=True, exist_ok=True)
        (carpeta / f"muestra_denue_{etiqueta}.zip").write_bytes(
            zip_version(grupo[COLUMNAS], etiqueta))
        conteo[etiqueta] = len(grupo)

    carpeta = destino / "comtrade"
    carpeta.mkdir(parents=True, exist_ok=True)
    meses = 0
    for ruta in sorted(raw_comtrade.glob("*.json")):
        datos = json.loads(ruta.read_text(encoding="utf-8"))
        compacto = {"data": [{k: fila.get(k) for k in CAMPOS_COMTRADE}
                             for fila in datos.get("data", [])]}
        (carpeta / ruta.name).write_text(json.dumps(compacto), encoding="utf-8")
        meses += 1
    return {"denue_filas_por_version": conteo, "comtrade_meses": meses,
            "metropolis": list(nombres_metropoli)}
