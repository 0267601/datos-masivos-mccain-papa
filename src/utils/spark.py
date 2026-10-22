"""
Creacion y cierre de la SparkSession.

Local: usamos la misma configuracion que en los notebooks de clase, modo local
con todos los nucleos y pocas particiones de shuffle, porque nuestro volumen
cabe en una sola maquina y 200 particiones (el valor por defecto) solo agregan
overhead de tareas vacias.

Databricks: la sesion ya existe y la administra la plataforma. En serverless
no podemos fijar master ni memoria del driver, asi que tomamos la sesion
activa y aplicamos solo las configuraciones que se permiten.
"""

from pyspark.sql import SparkSession

from src.utils.rutas import EN_DATABRICKS

# Spark 4 activa ANSI por defecto: un cast invalido truena en lugar de
# devolver null. Lo apagamos para que el pipeline se comporte igual en
# Spark 3.5 (Colab), 4.x local y Databricks, y contamos los nulos en la
# validacion.
CONFIG_SESION = {
    "spark.sql.session.timeZone": "America/Mexico_City",
    "spark.sql.ansi.enabled": "false",
}


def crear_spark(app="DatosMasivos-McCain", particiones_shuffle=8, memoria="2g"):
    """Creamos (o recuperamos) la SparkSession del proyecto."""
    if EN_DATABRICKS:
        spark = SparkSession.builder.getOrCreate()
        configuracion = {**CONFIG_SESION,
                         "spark.sql.shuffle.partitions": str(particiones_shuffle)}
        for clave, valor in configuracion.items():
            try:
                spark.conf.set(clave, valor)
            except Exception:       # serverless rechaza algunas configuraciones
                pass
        return spark

    builder = (
        SparkSession.builder
        .appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(particiones_shuffle))
        .config("spark.driver.memory", memoria)
        .config("spark.ui.showConsoleProgress", "false")
    )
    for clave, valor in CONFIG_SESION.items():
        builder = builder.config(clave, valor)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def cerrar_spark(spark):
    """Cerramos la sesion solo en local: en Databricks es la del notebook."""
    if not EN_DATABRICKS:
        spark.stop()


def intentar_cache(df):
    """Guardamos el DataFrame en memoria si el entorno lo permite.

    Serverless de Databricks no permite cache(); ahi seguimos sin el.
    """
    if EN_DATABRICKS:
        return df
    try:
        return df.cache()
    except Exception:
        return df
