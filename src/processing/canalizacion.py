"""
Indice de canalizacion por estado y anio — cruce SIAP x SNIIM.

Es el mismo indicador del entregable 1, ahora calculado dentro del pipeline,
para todos los anios donde tenemos ambas fuentes, y con Spark:

    canalizacion = % de la produccion nacional  -  % de las cotizaciones mayoristas

Positivo: el estado produce mas de lo que aparece en el mercado spot, su papa
sale por contrato (lo que buscamos). Negativo: aparece mas en mayoreo de lo
que produce (redistribucion, como Coahuila con La Laguna).

Excluimos la papa importada del denominador de mayoreo: no tiene estado de
origen y no compite por el indice.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def construir_canalizacion(siap, sniim):
    """Devolvemos un DataFrame anio-estado con el indice y sus componentes."""
    produccion = (
        siap
        .groupBy("anio", "estado_llave")
        .agg(
            F.first("estado").alias("estado_siap"),
            F.sum("produccion_ton").alias("produccion_ton"),
            F.sum("valor_produccion").alias("valor_produccion"),
        )
    )

    mayoreo = (
        sniim
        .filter(~F.col("es_importacion"))
        .groupBy("anio", F.col("estado_origen_llave").alias("estado_llave"))
        .agg(
            F.first("estado_origen").alias("estado_sniim"),
            F.count("*").alias("cotizaciones"),
            F.avg("precio_frec").alias("precio_frec_promedio"),
        )
    )

    # Solo comparamos anios donde existen ambas fuentes.
    anios_comunes = produccion.select("anio").distinct().join(
        mayoreo.select("anio").distinct(), "anio"
    )

    # La lista de anios comunes es diminuta: la copiamos a cada executor
    # (broadcast) para filtrar sin shuffle. El cruce final es full outer
    # (estados que producen sin cotizar y al reves), y Spark no puede
    # resolver un full outer con broadcast: ahi si hay Exchange.
    produccion = produccion.join(F.broadcast(anios_comunes), "anio")
    mayoreo = mayoreo.join(F.broadcast(anios_comunes), "anio")

    cruce = (
        produccion
        .join(mayoreo, ["anio", "estado_llave"], "full_outer")
        .withColumn("estado", F.coalesce("estado_siap", "estado_sniim"))
        .fillna({"produccion_ton": 0.0, "valor_produccion": 0.0, "cotizaciones": 0})
    )

    por_anio = Window.partitionBy("anio")
    ranking = Window.partitionBy("anio").orderBy(F.desc("canalizacion"))

    return (
        cruce
        .withColumn("pct_nacional",
                    100 * F.col("produccion_ton") / F.sum("produccion_ton").over(por_anio))
        .withColumn("pct_mayoreo",
                    100 * F.col("cotizaciones") / F.sum("cotizaciones").over(por_anio))
        .withColumn("canalizacion", F.col("pct_nacional") - F.col("pct_mayoreo"))
        .withColumn("precio_rural_ton",
                    F.when(F.col("produccion_ton") > 0,
                           F.col("valor_produccion") / F.col("produccion_ton")))
        .withColumn("ranking", F.row_number().over(ranking))
        .select("anio", "ranking", "estado", "estado_llave", "produccion_ton",
                "pct_nacional", "cotizaciones", "pct_mayoreo", "canalizacion",
                "precio_rural_ton", "precio_frec_promedio")
    )
