"""
Procesamiento de Las metropolis de Mexico 2020 (CONAPO) con PySpark.

Son dos tablas chicas que nos sirven de catalogo:
    municipios       -> que municipio pertenece a que zona metropolitana, con
                        su poblacion 2020 (422 municipios en 92 metropolis)
    caracteristicas  -> una fila por metropoli: poblacion, crecimiento,
                        PIB y poblacion ocupada

La llave de cruce con el DENUE es la clave de municipio a 5 digitos. CONAPO
la trae como numero (1001), el DENUE como texto con ceros ("01" + "001").
La unificamos en cve_mun5 = "01001".

Pipeline:
    CSV -> Spark -> tipos y llave cve_mun5 -> Parquet processed
"""

from pyspark.sql import functions as F


def leer_csv(spark, ruta):
    return (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("inferSchema", False)
        .csv(str(ruta))
    )


def construir_municipios(spark, ruta):
    return (
        leer_csv(spark, ruta)
        .select(
            F.trim("clave_metropoli").alias("clave_metropoli"),
            F.trim("tipo").alias("tipo_metropoli"),
            F.trim("nombre").alias("metropoli"),
            F.lpad(F.trim("clave_compuesta_municipio"), 5, "0").alias("cve_mun5"),
            F.trim("municipio").alias("municipio"),
            F.trim("entidad_etq").alias("entidad"),
            F.col("poblacion_2020").cast("long").alias("poblacion_2020"),
            F.col("tasa_crecimiento_medio_anual_2010_2020").cast("double")
             .alias("tasa_pob_2010_2020"),
        )
        .dropDuplicates(["cve_mun5"])
    )


def construir_metropolis(spark, ruta):
    return (
        leer_csv(spark, ruta)
        .select(
            F.trim("clave_metropoli").alias("clave_metropoli"),
            F.trim("tipo").alias("tipo_metropoli"),
            F.trim("nombre").alias("metropoli"),
            F.col("poblacion").cast("long").alias("poblacion_2020"),
            F.col("tasa_crecimiento_anual_2010_2020_prct").cast("double")
             .alias("tasa_pob_2010_2020"),
            F.col("producto_interno_bruto").cast("double").alias("pib"),
            F.col("poblacion_ocupada").cast("long").alias("poblacion_ocupada"),
        )
        .withColumn("pib_per_capita", F.col("pib") / F.col("poblacion_2020"))
        .dropDuplicates(["clave_metropoli"])
    )
