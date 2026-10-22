"""
Creacion de la SparkSession.

Usamos la misma configuracion que en los notebooks de clase: modo local con
todos los nucleos y pocas particiones de shuffle, porque nuestro volumen cabe
en una sola maquina y 200 particiones (el valor por defecto) solo agregan
overhead de tareas vacias.
"""

from pyspark.sql import SparkSession


def crear_spark(app="DatosMasivos-McCain", particiones_shuffle=8, memoria="2g"):
    """Creamos (o recuperamos) la SparkSession del proyecto."""
    spark = (
        SparkSession.builder
        .appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(particiones_shuffle))
        .config("spark.driver.memory", memoria)
        .config("spark.sql.session.timeZone", "America/Mexico_City")
        .config("spark.ui.showConsoleProgress", "false")
        # Spark 4 activa ANSI por defecto: un cast invalido truena en lugar de
        # devolver null. Lo apagamos para que el pipeline se comporte igual en
        # Spark 3.5 (Colab) y 4.x, y contamos los nulos en la validacion.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
