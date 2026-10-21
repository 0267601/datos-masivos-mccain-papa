"""
Procesamiento de SNIIM con PySpark: raw -> interim -> processed.

raw -> interim
    Los crudos son HTML (uno por variedad y mes). En lugar de parsearlos uno
    por uno en un ciclo de Python, los leemos con wholeTextFiles y repartimos
    el parseo con BeautifulSoup entre las particiones (flatMap). Con 72
    archivos da igual; con varios anios y variedades, el parseo deja de ser un
    cuello de botella porque corre en paralelo.
    En interim dejamos los registros tal como vienen en la tabla: todo texto,
    mas la variedad, el ProductoId y el archivo de origen (trazabilidad).

interim -> processed
    Tipos (fecha y precios), estado y mercado destino separados, estado de
    origen estandarizado, bandera de importacion, duplicados y nulos.
    Escribimos Parquet particionado por anio y mes: casi todas nuestras
    consultas filtran por periodo, asi Spark puede saltarse carpetas enteras
    (partition pruning).

Pipeline:
    HTML -> wholeTextFiles -> flatMap(BeautifulSoup) -> Parquet interim
         -> tipos/limpieza/variables -> Parquet processed (anio=/mes=)
"""

import re

from pyspark.sql import Row
from pyspark.sql import functions as F

from src.ingestion.sniim_extractor import CAMPOS
from src.processing.siap import estandarizar_estado, llave_estado

LLAVE_SNIIM = ["fecha", "variedad", "origen", "destino", "presentacion"]

# sniim_<ProductoId>_<anio>-<mes>.html dentro de la carpeta de la variedad
PATRON_ARCHIVO = re.compile(r"/([^/]+)/sniim_(\d+)_(\d{4})-(\d{2})\.html$")

ESQUEMA_INTERIM = CAMPOS + ["variedad_carpeta", "producto_id", "archivo"]


def parsear_archivo(ruta_y_html):
    """Recibimos (ruta, html) y devolvemos sus registros como Rows.

    Importamos dentro de la funcion para que cada executor cargue
    BeautifulSoup por su cuenta.
    """
    from src.ingestion.sniim_extractor import parsear_html

    ruta, html = ruta_y_html
    coincidencia = PATRON_ARCHIVO.search(ruta)
    if coincidencia is None:
        return []

    carpeta, producto_id, _, _ = coincidencia.groups()
    nombre = ruta.rsplit("/", 1)[-1]

    filas = []
    for registro in parsear_html(html, variedad=carpeta):
        registro.pop("variedad")
        filas.append(Row(**registro, variedad_carpeta=carpeta,
                         producto_id=producto_id, archivo=nombre))
    return filas


def construir_interim(spark, carpeta_cruda, variedades):
    """Parseamos todos los HTML crudos en paralelo y devolvemos un DataFrame de texto.

    variedades: {ProductoId: nombre}. Recuperamos el nombre bonito ("San Jose")
    a partir del ProductoId, no de la carpeta ("san_jose").
    """
    patron = str(carpeta_cruda / "*" / "*.html")
    archivos = spark.sparkContext.wholeTextFiles(patron)

    filas = archivos.flatMap(parsear_archivo)
    if filas.isEmpty():
        raise FileNotFoundError(
            "No hay HTML crudos de SNIIM en data/raw/sniim/. "
            "Primero: python -m src.ingestion.download_data"
        )

    df = spark.createDataFrame(filas).select(*ESQUEMA_INTERIM)

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
