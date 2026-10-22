"""
Indice de inversion por zona metropolitana.

Pregunta:
    Si McCain tuviera X millones para crecer en Mexico, ¿en que zonas
    metropolitanas deberia ponerlos?

Ejecucion desde la raiz del repositorio (despues de build_dataset):

    python -m src.analysis.indice_inversion

Como lo calculamos, para cada una de las 92 metropolis de CONAPO:

    A. Crecimiento   tasa anual de restaurantes donde se venden papas
                     (comida rapida, autoservicio, a la carta) entre dos
                     versiones del DENUE
    B. Cadenas       aperturas netas de cadenas que venden papas (McDonald's,
                     KFC, Carl's Jr...) por cada 100 mil habitantes
    Turismo          hoteles por cada 100 mil habitantes
    Mercado          numero de restaurantes objetivo hoy (tamano)

Cada componente se convierte en un puntaje de 0 a 100 con su percentil entre
las 92 metropolis (asi no domina el que tiene numeros mas grandes) y el indice
es el promedio ponderado. Los pesos estan en config/config.example.yaml.

La pieza C (competencia) es nacional: Comtrade no separa por ciudad. La
resumimos aparte: cuanto se importa de cada pais y a que precio por kilo.

Escribimos:
    data/processed/indice_inversion/       una fila por metropoli
    data/processed/serie_metropolis/       restaurantes, cadenas y hoteles por version
    data/processed/cadenas_metropolis/     sucursales por cadena, metropoli y version
    data/processed/importaciones_anuales/  pais x anio: valor, kilos, usd/kg
    reports/tables/indice_inversion.csv    la tabla para leer en Excel
    reports/figures/*.png                  las tres graficas

Pipeline:
    DENUE + CONAPO -> conteos por metropoli y version (Spark, broadcast)
                   -> componentes -> percentiles (window) -> indice
"""

import sys
import time
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.config import cargar_config
from src.utils.registro import configurar_registro
from src.utils.rutas import PROCESSED, TABLAS, asegurar_carpetas
from src.utils.spark import cerrar_spark, crear_spark

log = configurar_registro("analysis")

SALIDAS = {
    "indice": PROCESSED / "indice_inversion",
    "serie": PROCESSED / "serie_metropolis",
    "cadenas": PROCESSED / "cadenas_metropolis",
    "importaciones_anuales": PROCESSED / "importaciones_anuales",
}

COMPONENTES = {
    "crecimiento": "crecimiento_anual",
    "cadenas": "cadenas_netas_100k",
    "turismo": "hoteles_100k",
    "mercado": "objetivo_final",
}


def serie_por_metropoli(denue, municipios, categorias_objetivo):
    """Conteos por metropoli y version. Solo municipios metropolitanos."""
    es_objetivo = F.col("categoria").isin(categorias_objetivo)
    return (
        # El catalogo de municipios es chico (~420 filas): broadcast, sin shuffle
        # de la tabla grande del DENUE.
        denue.join(F.broadcast(municipios.select("cve_mun5", "clave_metropoli", "metropoli")),
                   "cve_mun5")
        .groupBy("clave_metropoli", "metropoli", "version", "fecha_corte")
        .agg(
            F.sum(es_objetivo.cast("int")).alias("objetivo"),
            F.sum((F.col("categoria") == "comida_rapida").cast("int")).alias("comida_rapida"),
            F.sum((F.col("categoria") == "autoservicio").cast("int")).alias("autoservicio"),
            F.sum((F.col("categoria") == "a_la_carta").cast("int")).alias("a_la_carta"),
            F.sum(F.col("cadena").isNotNull().cast("int")).alias("cadenas"),
            F.sum((F.col("categoria") == "hoteles").cast("int")).alias("hoteles"),
            F.count("*").alias("sector_72"),
        )
    )


def cadenas_por_metropoli(denue, municipios):
    return (
        denue.filter(F.col("cadena").isNotNull())
        .join(F.broadcast(municipios.select("cve_mun5", "clave_metropoli", "metropoli")),
              "cve_mun5")
        .groupBy("clave_metropoli", "metropoli", "version", "cadena")
        .agg(F.count("*").alias("sucursales"))
    )


def calcular_indice(serie, metropolis, version_inicial, version_final, pesos):
    """Componentes por metropoli, percentiles y el indice ponderado."""
    ini = serie.filter(F.col("version") == version_inicial).select(
        "clave_metropoli",
        F.col("fecha_corte").alias("fecha_inicial"),
        F.col("objetivo").alias("objetivo_inicial"),
        F.col("cadenas").alias("cadenas_inicial"),
    )
    fin = serie.filter(F.col("version") == version_final).select(
        "clave_metropoli", "metropoli",
        F.col("fecha_corte").alias("fecha_final"),
        F.col("objetivo").alias("objetivo_final"),
        F.col("comida_rapida").alias("comida_rapida_final"),
        F.col("cadenas").alias("cadenas_final"),
        F.col("hoteles").alias("hoteles_final"),
    )

    anios = F.months_between("fecha_final", "fecha_inicial") / 12
    por_100k = 100_000 / F.col("poblacion_2020")

    base = (
        fin.join(ini, "clave_metropoli", "left")
        .join(F.broadcast(metropolis.select("clave_metropoli", "tipo_metropoli",
                                            "poblacion_2020", "pib_per_capita")),
              "clave_metropoli", "left")
        .withColumn("anios", anios)
        .withColumn("crecimiento_anual", F.when(
            F.col("objetivo_inicial") > 0,
            100 * (F.pow(F.col("objetivo_final") / F.col("objetivo_inicial"),
                         1 / F.col("anios")) - 1)))
        .withColumn("cadenas_netas_100k",
                    (F.col("cadenas_final") - F.coalesce("cadenas_inicial", F.lit(0))) * por_100k)
        .withColumn("hoteles_100k", F.col("hoteles_final") * por_100k)
        .withColumn("restaurantes_100k", F.col("objetivo_final") * por_100k)
    )

    # Percentil de cada componente entre todas las metropolis (0 a 100).
    for nombre, columna in COMPONENTES.items():
        ventana = Window.orderBy(F.col(columna).asc_nulls_first())
        base = base.withColumn(f"puntaje_{nombre}", F.round(100 * F.percent_rank().over(ventana), 1))

    indice = sum(F.col(f"puntaje_{n}") * float(pesos[n]) for n in COMPONENTES) / sum(
        float(pesos[n]) for n in COMPONENTES)

    ranking = Window.orderBy(F.desc("indice"))
    return (
        base.withColumn("indice", F.round(indice, 1))
        .withColumn("ranking", F.row_number().over(ranking))
        .select("ranking", "clave_metropoli", "metropoli", "tipo_metropoli", "indice",
                *[f"puntaje_{n}" for n in COMPONENTES],
                "crecimiento_anual", "cadenas_netas_100k", "hoteles_100k",
                "objetivo_inicial", "objetivo_final", "comida_rapida_final",
                "cadenas_inicial", "cadenas_final", "hoteles_final",
                "restaurantes_100k", "poblacion_2020", "pib_per_capita",
                "fecha_inicial", "fecha_final")
    )


def explicar(fila):
    """Una frase con las dos fortalezas mas altas de la metropoli."""
    nombres = {"crecimiento": "crecimiento de restaurantes", "cadenas": "aperturas de cadenas",
               "turismo": "turismo", "mercado": "tamano de mercado"}
    puntajes = sorted(((fila[f"puntaje_{n}"] or 0, n) for n in COMPONENTES), reverse=True)
    return " y ".join(nombres[n] for _, n in puntajes[:2])


def main():
    asegurar_carpetas()
    config = cargar_config()
    cfg = config["indice"]
    spark = crear_spark("DatosMasivos-Indice")
    inicio = time.perf_counter()

    rutas = {n: PROCESSED / n for n in ["denue_establecimientos", "metropolis_municipios",
                                        "metropolis"]}
    faltan = [str(r) for r in rutas.values() if not r.exists()]
    if faltan:
        log.error(f"No existe {faltan}. Primero: python -m src.processing.build_dataset")
        return 1

    denue = spark.read.parquet(str(rutas["denue_establecimientos"]))
    municipios = spark.read.parquet(str(rutas["metropolis_municipios"]))
    metropolis = spark.read.parquet(str(rutas["metropolis"]))

    serie = serie_por_metropoli(denue, municipios, config["denue"]["categorias_objetivo"])
    serie.write.mode("overwrite").parquet(str(SALIDAS["serie"]))
    serie = spark.read.parquet(str(SALIDAS["serie"]))

    versiones = {r[0] for r in serie.select("version").distinct().collect()}
    for etiqueta in (cfg["version_inicial"], cfg["version_final"]):
        if etiqueta not in versiones:
            log.error(f"Falta la version {etiqueta} del DENUE. Hay: {sorted(versiones)}")
            return 1

    cadenas_por_metropoli(denue, municipios).write.mode("overwrite").parquet(
        str(SALIDAS["cadenas"]))

    indice = calcular_indice(serie, metropolis, cfg["version_inicial"],
                             cfg["version_final"], cfg["pesos"])
    indice.coalesce(1).write.mode("overwrite").parquet(str(SALIDAS["indice"]))

    ruta_importaciones = PROCESSED / "importaciones"
    if ruta_importaciones.exists():
        from src.processing.comtrade import resumir_anual
        resumir_anual(spark.read.parquet(str(ruta_importaciones))).coalesce(1).write.mode(
            "overwrite").parquet(str(SALIDAS["importaciones_anuales"]))
    else:
        log.warning("Sin importaciones de Comtrade: la pieza de competencia queda fuera")

    # Tabla y graficas: ya son tablas chicas, aqui si usamos pandas.
    tabla = spark.read.parquet(str(SALIDAS["indice"])).toPandas().sort_values("ranking")
    tabla["fortalezas"] = tabla.apply(explicar, axis=1)
    tabla.to_csv(TABLAS / "indice_inversion.csv", index=False, encoding="utf-8")

    from src.analysis import graficas
    graficas.generar_todas(spark)

    for _, fila in tabla.head(10).iterrows():
        log.info(f"{fila.ranking:2d}. {fila.metropoli:28s} indice {fila.indice:5.1f} "
                 f"| crecimiento {fila.crecimiento_anual or 0:5.1f}%/anio "
                 f"| {fila.fortalezas}")
    log.info(f"Indice de {len(tabla)} metropolis en {time.perf_counter() - inicio:.1f} s "
             f"-> {TABLAS / 'indice_inversion.csv'} ({date.today()})")
    cerrar_spark(spark)
    return 0


if __name__ == "__main__":
    sys.exit(main())
