"""
Procesamiento de SIAP con PySpark: raw -> interim -> processed.

raw -> interim
    Leemos cada CSV crudo por separado y unificamos su esquema antes de juntar.
    No podemos leer los 13 anios de un jalon con spark.read.csv(carpeta): Spark
    toma el encabezado del primer archivo para todos, y como los encabezados
    cambian entre anios (schema drift), las columnas quedarian desalineadas
    sin ningun error visible. Por eso leemos anio por anio, renombramos y
    juntamos con unionByName.
    En interim dejamos solo papa, con nombres unificados, todo como texto.

interim -> processed
    Tipos, nombres en snake_case, llave de estado estandarizada, duplicados,
    nulos en columnas llave y variables derivadas. Escribimos Parquet
    particionado por anio.

Pipeline:
    CSV latin-1 (13) -> Spark -> normalizar -> union -> Parquet interim
                     -> tipos/limpieza/variables -> Parquet processed (anio=)
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ingestion.siap_extractor import COLUMNAS_NUMERICAS, COLUMNAS_UTILES, RENOMBRES

# Nombres finales en snake_case. Ponemos la unidad en el nombre para que nadie
# tenga que adivinar si una superficie viene en hectareas o una produccion en kg.
NOMBRES_PROCESSED = {
    "Anio": "anio",
    "Nomestado": "estado",
    "Nomddr": "ddr",
    "Nomcader": "cader",
    "Nommunicipio": "municipio",
    "Nomcicloproductivo": "ciclo",
    "Nommodalidad": "modalidad",
    "Nomcultivo": "cultivo",
    "Sembrada": "sup_sembrada_ha",
    "Cosechada": "sup_cosechada_ha",
    "Siniestrada": "sup_siniestrada_ha",
    "Volumenproduccion": "produccion_ton",
    "Rendimiento": "rendimiento_ton_ha",
    "Preciomediorural": "precio_medio_rural",
    "Valorproduccion": "valor_produccion",
}

# Conservamos ddr y cader: sin ellas la llave no es unica (lo vimos en el E1,
# 186 falsos duplicados).
LLAVE_SIAP = ["anio", "estado", "ddr", "cader", "municipio", "ciclo", "modalidad"]

# La capital aparece con cuatro nombres distintos entre fuentes y anios:
#   SIAP  -> "Ciudad de México" y, en algunos anios, "Ciudad de México / DF"
#   SNIIM -> "Distrito Federal" como origen y "DF" como destino
# Si no los unificamos, el cruce la parte en varios estados.
ESTADOS_EQUIVALENTES = {
    "Ciudad de México / DF": "Ciudad de México",
    "Distrito Federal": "Ciudad de México",
    "DF": "Ciudad de México",
}


def llave_estado(columna):
    """Llave para cruzar estados entre fuentes: minusculas, sin acentos, sin espacios extra.

    SIAP y SNIIM escriben igual casi todos los estados, pero no todos
    ("Baja California " con espacio en SNIIM, acentos que a veces faltan).
    """
    return F.lower(F.trim(F.translate(columna, "ÁÉÍÓÚÜÑáéíóúüñ", "AEIOUUNaeiouun")))


def estandarizar_estado(columna):
    """Aplicamos las equivalencias de nombre y quitamos espacios."""
    expr = F.trim(columna)
    for viejo, nuevo in ESTADOS_EQUIVALENTES.items():
        expr = F.when(expr == viejo, F.lit(nuevo)).otherwise(expr)
    return expr


# --- raw -> interim -------------------------------------------------------------

def leer_anio_crudo(spark, ruta):
    """Leemos un CSV crudo de SIAP y unificamos su esquema.

    Leemos todo como texto (sin inferSchema): los tipos los asignamos nosotros
    en processed, para no heredar el "object" que contamina el CSV completo.
    """
    df = (
        spark.read
        .option("header", True)
        .option("encoding", "ISO-8859-1")      # el portal sirve latin-1
        .option("inferSchema", False)
        .csv(str(ruta))
    )

    # Quitamos espacios de los encabezados y unificamos los que cambiaron de nombre.
    df = df.toDF(*[c.strip() for c in df.columns])
    for viejo, nuevo in RENOMBRES.items():
        if viejo in df.columns:
            df = df.withColumnRenamed(viejo, nuevo)

    # Si a un anio le falta una columna, la creamos vacia para que unionByName
    # no truene y la ausencia quede visible como nulos en la validacion.
    for columna in COLUMNAS_UTILES:
        if columna not in df.columns:
            df = df.withColumn(columna, F.lit(None).cast("string"))

    return df.select(*COLUMNAS_UTILES)


def construir_interim(spark, rutas_crudas, cultivo="papa"):
    """Juntamos todos los anios crudos y nos quedamos con el cultivo."""
    if not rutas_crudas:
        raise FileNotFoundError(
            "No hay CSV crudos de SIAP en data/raw/siap/. "
            "Primero: python -m src.ingestion.download_data"
        )

    partes = [leer_anio_crudo(spark, ruta) for ruta in sorted(rutas_crudas)]
    df = reduce(DataFrame.unionByName, partes)

    return df.filter(F.lower(F.trim(F.col("Nomcultivo"))) == cultivo.lower())


# --- interim -> processed -------------------------------------------------------

def a_numero(columna):
    """Texto -> double. Quitamos comas de miles; lo que no sea numero queda null."""
    return F.regexp_replace(F.trim(F.col(columna)), ",", "").cast("double")


def construir_processed(df_interim):
    """Asignamos tipos, limpiamos y derivamos variables.

    Devolvemos (DataFrame, metricas) para reportar cuantas filas se fueron en
    cada paso, igual que hicimos con las metricas de descarga en el E1.
    """
    metricas = {"filas_interim": df_interim.count()}

    df = df_interim
    for columna in COLUMNAS_NUMERICAS:
        df = df.withColumn(columna, a_numero(columna))

    df = (
        df
        .withColumn("Anio", F.col("Anio").cast("int"))
        .withColumn("Nomestado", estandarizar_estado(F.col("Nomestado")))
    )

    for columna in ["Nomddr", "Nomcader", "Nommunicipio", "Nomcicloproductivo",
                    "Nommodalidad", "Nomcultivo"]:
        df = df.withColumn(columna, F.trim(F.col(columna)))

    df = df.select(*[F.col(viejo).alias(nuevo) for viejo, nuevo in NOMBRES_PROCESSED.items()])

    # Nulos: sin anio, estado o produccion la fila no sirve para el analisis.
    df_sin_nulos = df.dropna(subset=["anio", "estado", "produccion_ton"])
    metricas["filas_nulos_llave"] = metricas["filas_interim"] - df_sin_nulos.count()

    # Duplicados exactos por la llave completa del registro.
    df_unico = df_sin_nulos.dropDuplicates(LLAVE_SIAP)
    metricas["filas_duplicadas"] = (metricas["filas_interim"]
                                    - metricas["filas_nulos_llave"]
                                    - df_unico.count())

    df_final = (
        df_unico
        .withColumn("estado_llave", llave_estado(F.col("estado")))
        # Variables derivadas
        .withColumn(
            "pct_siniestrada",
            F.when(F.col("sup_sembrada_ha") > 0,
                   100 * F.col("sup_siniestrada_ha") / F.col("sup_sembrada_ha")),
        )
        .withColumn(
            "valor_por_ton",
            F.when(F.col("produccion_ton") > 0,
                   F.col("valor_produccion") / F.col("produccion_ton")),
        )
    )

    metricas["filas_processed"] = df_final.count()
    return df_final, metricas
