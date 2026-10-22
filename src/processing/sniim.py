"""
Procesamiento de SNIIM con PySpark: raw -> interim -> processed.

raw -> interim
    Los crudos son HTML (uno por variedad y mes). En lugar de parsearlos uno
    por uno en un ciclo de Python, los leemos completos con
    spark.read.text(wholetext=True) y repartimos el parseo con BeautifulSoup
    entre las particiones con una UDF. Con 72 archivos da igual; con varios
    anios y variedades, el parseo deja de ser un cuello de botella porque
    corre en paralelo.
    No usamos wholeTextFiles (API de RDD): Databricks serverless no expone
    RDDs, y con la API de DataFrame el mismo codigo corre local y alla.
    En interim dejamos los registros tal como vienen en la tabla: todo texto,
    mas la variedad, el ProductoId y el archivo de origen (trazabilidad).

interim -> processed
    Tipos (fecha y precios), estado y mercado destino separados, estado de
    origen estandarizado, bandera de importacion, duplicados y nulos.
    Escribimos Parquet particionado por anio y mes: casi todas nuestras
    consultas filtran por periodo, asi Spark puede saltarse carpetas enteras
    (partition pruning).

Pipeline:
    HTML -> read.text(wholetext) -> UDF(BeautifulSoup) -> explode -> Parquet interim
         -> tipos/limpieza/variables -> Parquet processed (anio=/mes=)
"""

import re
import sys

from pyspark import cloudpickle
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.ingestion import sniim_extractor
from src.ingestion.sniim_extractor import CAMPOS, parsear_html
from src.processing.siap import estandarizar_estado, llave_estado

# La UDF corre en los workers. En local comparten la carpeta del repo, pero en
# Databricks serverless no: ahi "import src" falla. Le pedimos a cloudpickle
# que mande este modulo y el del extractor completos (por valor) junto con la
# UDF, en lugar de solo su nombre. Asi el worker solo necesita BeautifulSoup.
cloudpickle.register_pickle_by_value(sys.modules[__name__])
cloudpickle.register_pickle_by_value(sniim_extractor)

LLAVE_SNIIM = ["fecha", "variedad", "origen", "destino", "presentacion"]

# sniim_<ProductoId>_<anio>-<mes>.html dentro de la carpeta de la variedad
PATRON_ARCHIVO = re.compile(r"/([^/]+)/sniim_(\d+)_(\d{4})-(\d{2})\.html$")

ESQUEMA_INTERIM = CAMPOS + ["variedad_carpeta", "producto_id", "archivo"]

# Lo que devuelve la UDF por archivo: una lista de registros, todo texto.
TIPO_REGISTROS = T.ArrayType(T.StructType(
    [T.StructField(c, T.StringType()) for c in ESQUEMA_INTERIM]
))


def parsear_archivo(ruta, html):
    """Recibimos la ruta y el HTML de un archivo y devolvemos sus registros."""
    coincidencia = PATRON_ARCHIVO.search(ruta or "")
    if coincidencia is None or html is None:
        return []

    carpeta, producto_id, _, _ = coincidencia.groups()
    nombre = ruta.rsplit("/", 1)[-1]

    filas = []
    for registro in parsear_html(html, variedad=carpeta):
        registro.pop("variedad")
        filas.append({**registro, "variedad_carpeta": carpeta,
                      "producto_id": producto_id, "archivo": nombre})
    return filas


parsear_udf = F.udf(parsear_archivo, TIPO_REGISTROS)


def construir_interim(spark, carpeta_cruda, variedades):
    """Parseamos todos los HTML crudos en paralelo y devolvemos un DataFrame de texto.

    variedades: {ProductoId: nombre}. Recuperamos el nombre bonito ("San Jose")
    a partir del ProductoId, no de la carpeta ("san_jose").
    """
    patron = str(carpeta_cruda / "*" / "*.html")
    sin_datos = FileNotFoundError(
        f"No hay HTML crudos de SNIIM en {carpeta_cruda}. "
        "Primero: python -m src.ingestion.download_data"
    )

    if not list(carpeta_cruda.glob("*/*.html")):
        raise sin_datos

    archivos = (
        spark.read
        # wholetext como argumento y no con .option(): con .option("wholetext", True)
        # Spark Connect (Databricks serverless) lo ignora y lee renglon por renglon.
        .text(patron, wholetext=True)               # un renglon por archivo
        .select(F.col("_metadata.file_path").alias("ruta"),
                F.col("value").alias("html"))
    )

    df = (
        archivos
        .select(F.explode(parsear_udf("ruta", "html")).alias("r"))
        .select("r.*")
    )
    if df.isEmpty():
        raise sin_datos

    nombres = F.create_map(*[x for pid, nombre in variedades.items()
                             for x in (F.lit(str(pid)), F.lit(nombre))])

    return (
        df
        .withColumn("variedad", F.coalesce(nombres[F.col("producto_id")],
                                           F.col("variedad_carpeta")))
        .drop("variedad_carpeta")
    )


def construir_processed(df_interim):
    """Asignamos tipos, limpiamos y derivamos variables. Devolvemos (df, metricas)."""
    metricas = {"filas_interim": df_interim.count()}

    df = df_interim
    for columna in ["presentacion", "origen", "destino", "obs", "variedad"]:
        df = df.withColumn(columna, F.trim(F.col(columna)))

    df = (
        df
        .withColumn("fecha", F.to_date(F.trim(F.col("fecha")), "dd/MM/yyyy"))
        .withColumn("precio_min", F.col("precio_min").cast("double"))
        .withColumn("precio_max", F.col("precio_max").cast("double"))
        .withColumn("precio_frec", F.col("precio_frec").cast("double"))
        .withColumn("producto_id", F.col("producto_id").cast("int"))
        .withColumn("obs", F.when(F.col("obs") == "", None).otherwise(F.col("obs")))
    )

    # Nulos: sin fecha o sin precio frecuente la cotizacion no sirve.
    df_sin_nulos = df.dropna(subset=["fecha", "precio_frec"])
    metricas["filas_nulos_llave"] = metricas["filas_interim"] - df_sin_nulos.count()

    df_unico = df_sin_nulos.dropDuplicates(LLAVE_SNIIM)
    metricas["filas_duplicadas"] = (metricas["filas_interim"]
                                    - metricas["filas_nulos_llave"]
                                    - df_unico.count())

    es_importacion = F.col("origen") == "Importación"

    df_final = (
        df_unico
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        # El destino viene como "Estado: Nombre del mercado".
        .withColumn("estado_destino",
                    estandarizar_estado(F.split(F.col("destino"), ":").getItem(0)))
        .withColumn("mercado", F.trim(F.regexp_replace(F.col("destino"), r"^[^:]*:", "")))
        .withColumn("es_importacion", es_importacion)
        # Para importacion no hay estado de origen: lo dejamos nulo a proposito.
        .withColumn("estado_origen",
                    F.when(es_importacion, None)
                     .otherwise(estandarizar_estado(F.col("origen"))))
        .withColumn("estado_origen_llave", llave_estado(F.col("estado_origen")))
        .withColumn("estado_destino_llave", llave_estado(F.col("estado_destino")))
        .withColumn("rango_precio", F.col("precio_max") - F.col("precio_min"))
    )

    metricas["filas_processed"] = df_final.count()
    return df_final, metricas
