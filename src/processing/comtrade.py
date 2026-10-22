"""
Procesamiento de UN Comtrade con PySpark: importaciones de papas congeladas.

Los crudos son un JSON por mes. Los leemos con Python (son chicos: ~130
archivos de unos KB), los convertimos en filas y Spark asigna tipos y
calcula el precio por kilo de cada pais de origen.

Separamos el renglon "Mundo" (pais 0, el total del mes) de los paises para no
contar doble al sumar.

Pipeline:
    JSON por mes -> registros -> Spark -> tipos y usd_kg -> Parquet processed
"""

import json

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.ingestion.comtrade_extractor import PAISES, registros

ESQUEMA = T.StructType([
    T.StructField("periodo", T.StringType()),
    T.StructField("pais_codigo", T.IntegerType()),
    T.StructField("pais", T.StringType()),
    T.StructField("valor_usd", T.DoubleType()),
    T.StructField("kg", T.DoubleType()),
])


def leer_crudos(carpeta):
    """Juntamos los registros de todos los meses descargados."""
    filas = []
    for ruta in sorted(carpeta.glob("*.json")):
        filas.extend(registros(json.loads(ruta.read_text(encoding="utf-8"))))
    if not filas:
        raise FileNotFoundError(
            f"No hay JSON de Comtrade en {carpeta}. "
            "Primero: python -m src.ingestion.download_data --fuente comtrade"
        )
    return filas


def construir_processed(spark, carpeta):
    filas = leer_crudos(carpeta)
    for fila in filas:
        # Preferimos el nombre en espanol cuando lo tenemos.
        fila["pais"] = PAISES.get(fila["pais_codigo"], fila["pais"])
        fila["valor_usd"] = float(fila["valor_usd"]) if fila["valor_usd"] is not None else None
        fila["kg"] = float(fila["kg"]) if fila["kg"] is not None else None

    return (
        spark.createDataFrame(filas, schema=ESQUEMA)
        .withColumn("anio", F.substring("periodo", 1, 4).cast("int"))
        .withColumn("mes", F.substring("periodo", 5, 2).cast("int"))
        .withColumn("fecha", F.to_date(F.concat_ws("-", "anio", "mes", F.lit("01"))))
        .withColumn("es_total", F.col("pais_codigo") == 0)
        .withColumn("usd_kg", F.when(F.col("kg") > 0, F.col("valor_usd") / F.col("kg")))
        .dropDuplicates(["periodo", "pais_codigo"])
    )


def resumir_anual(importaciones):
    """Por anio y pais: valor, kilos, precio por kilo y participacion."""
    paises = importaciones.filter(~F.col("es_total"))
    anual = (
        paises.groupBy("anio", "pais")
        .agg(F.sum("valor_usd").alias("valor_usd"), F.sum("kg").alias("kg"),
             F.countDistinct("mes").alias("meses"))
        .withColumn("usd_kg", F.col("valor_usd") / F.col("kg"))
    )
    totales = anual.groupBy("anio").agg(F.sum("valor_usd").alias("valor_total"))
    return (
        anual.join(totales, "anio")
        .withColumn("participacion", 100 * F.col("valor_usd") / F.col("valor_total"))
        .drop("valor_total")
    )
