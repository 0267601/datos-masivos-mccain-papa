"""
Comparacion de desempeno: formatos, motores y particionamiento.

Ejecucion desde la raiz del repositorio (despues de build_dataset):

    python -m src.processing.benchmark                     # factores 1, 10, 100
    python -m src.processing.benchmark --factores 1 10 100 400

Que medimos:

    A. Lectura CSV vs Parquet (pandas)            tiempo, RAM y tamano en disco
    B. Agregacion pandas vs Spark                 mismo groupBy, mismo resultado
    C. Spark particionado vs no particionado      filtro por mes (partition pruning)
    D. Spark con 4, 8 y 16 particiones de shuffle el ejercicio de clase, sobre el volumen mayor

Como simulamos volumen:
    Nuestro dataset real es chico (decenas de miles de filas). Para ver que
    pasa al crecer, replicamos las cotizaciones procesadas del SNIIM k veces
    (factor 1, 10, 100...). Las filas son reales; solo se repiten. Asi medimos
    escalabilidad sin inventar datos.

Como medimos:
    - Tiempo: mediana de 3 corridas con time.perf_counter().
    - RAM de pandas: corremos cada prueba en un proceso aparte y leemos su
      pico de memoria (ru_maxrss), menos lo que pesa el proceso solo con las
      librerias importadas. Asi no se mezclan pruebas entre si.
    - RAM de Spark: vive en la JVM, fuera del proceso de Python; no la
      medimos aqui (se puede ver en la Spark UI). La marcamos "n/d".

Resultados:
    reports/tables/desempeno.csv
    reports/figures/desempeno_volumen.svg
"""

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import time

import pandas as pd
from pyspark.sql import functions as F

from src.utils.registro import configurar_registro
from src.utils.rutas import FIGURAS, INTERIM, PROCESSED, RAIZ, TABLAS, asegurar_carpetas
from src.utils.spark import cerrar_spark, crear_spark, intentar_cache

log = configurar_registro("benchmark")

CARPETA = INTERIM / "benchmark"
REPETICIONES = 3

# Columnas que usamos en las pruebas: las mismas que en el analisis.
COLUMNAS = ["fecha", "variedad", "origen", "estado_origen", "es_importacion",
            "precio_min", "precio_max", "precio_frec", "mes"]


# --- Preparar volumenes -----------------------------------------------------------

def tamano_mb(ruta):
    if ruta.is_file():
        return ruta.stat().st_size / 1024**2
    return sum(p.stat().st_size for p in ruta.rglob("*") if p.is_file()) / 1024**2


def preparar_volumen(spark, base, factor):
    """Replicamos la base k veces y escribimos CSV, Parquet y Parquet particionado."""
    destino = CARPETA / f"x{factor}"
    if destino.exists():
        shutil.rmtree(destino)

    # Revolvemos las filas: si las copias quedan juntas, Parquet las comprime por
    # repeticion (RLE) y el tamano en disco sale irrealmente chico. Aun revueltas,
    # las filas se repiten, asi que el diccionario de Parquet sigue ayudando:
    # el tamano de Parquet aqui es optimista y lo decimos en docs/desempeno.md.
    df = (
        base.crossJoin(spark.range(factor).withColumnRenamed("id", "copia"))
        .drop("copia")
        .withColumn("fecha", F.col("fecha").cast("string"))
        .orderBy(F.rand(seed=42))
    )

    # Un solo archivo por formato, como lo recibiria pandas.
    df.coalesce(1).write.option("header", True).csv(str(destino / "csv"))
    df.coalesce(1).write.parquet(str(destino / "parquet"))
    df.write.partitionBy("mes").parquet(str(destino / "parquet_mes"))

    archivo_csv = next((destino / "csv").glob("*.csv"))
    archivo_parquet = next((destino / "parquet").glob("*.parquet"))
    return {
        "filas": df.count(),
        "csv": archivo_csv,
        "parquet": archivo_parquet,
        "parquet_dir": destino / "parquet",
        "parquet_mes": destino / "parquet_mes",
    }


# --- pandas en un proceso aparte -------------------------------------------------

PLANTILLA_PANDAS = """
import json, resource, sys, time
import pandas as pd, pyarrow
def rss_mb():
    pico = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return pico / 1024**2 if sys.platform == "darwin" else pico / 1024
base = rss_mb()
inicio = time.perf_counter()
{codigo}
segundos = time.perf_counter() - inicio
print(json.dumps({{"segundos": segundos, "ram_mb": rss_mb() - base, "resultado": resultado}}), flush=True)
# Salimos sin el cierre normal del interprete: el pool de hilos de pyarrow a
# veces se queda esperando al apagar y el proceso nunca termina.
import os; os._exit(0)
"""


def medir_pandas(codigo):
    """Corremos el codigo en un python nuevo y devolvemos la mediana de tiempo y RAM."""
    corridas = []
    for _ in range(REPETICIONES):
        salida = subprocess.run(
            [sys.executable, "-c", PLANTILLA_PANDAS.format(codigo=codigo)],
            capture_output=True, text=True, check=True, cwd=RAIZ,
            stdin=subprocess.DEVNULL, timeout=900,
        )
        corridas.append(json.loads(salida.stdout.strip().splitlines()[-1]))
    return {
        "segundos": statistics.median(c["segundos"] for c in corridas),
        "ram_mb": statistics.median(c["ram_mb"] for c in corridas),
        "resultado": corridas[-1]["resultado"],
    }


def medir_spark(funcion):
    """Medimos una accion de Spark: una corrida de calentamiento y luego la mediana."""
    funcion()                                   # calentamiento: JIT y cache de metadatos
    tiempos, resultado = [], None
    for _ in range(REPETICIONES):
        inicio = time.perf_counter()
        resultado = funcion()
        tiempos.append(time.perf_counter() - inicio)
    return {"segundos": statistics.median(tiempos), "resultado": resultado}


def plan_fisico(df):
    """Devolvemos el texto de explain(); funciona local y en Databricks."""
    import contextlib
    import io

    salida = io.StringIO()
    with contextlib.redirect_stdout(salida):
        df.explain(mode="formatted")
    return salida.getvalue()


# --- Pruebas ----------------------------------------------------------------------

def prueba_lectura(vol):
    """A. pandas lee el mismo dato en CSV y en Parquet."""
    filas = []
    for formato, lector in [("CSV", "read_csv"), ("Parquet", "read_parquet")]:
        ruta = vol[formato.lower()]
        m = medir_pandas(f"df = pd.{lector}(r'{ruta}'); resultado = len(df)")
        filas.append({
            "prueba": "A. lectura", "variante": formato, "motor": "pandas",
            "filas": vol["filas"], "tamano_mb": tamano_mb(ruta),
            "segundos": m["segundos"], "ram_mb": m["ram_mb"],
            "resultado": "Correcto" if m["resultado"] == vol["filas"] else "Distinto",
        })
    return filas


def prueba_agregacion(spark, vol):
    """B. El mismo groupBy en pandas y en Spark, leyendo Parquet."""
    codigo = (
        f"df = pd.read_parquet(r'{vol['parquet']}', columns=['variedad', 'precio_frec'])\n"
        "agg = df.groupby('variedad').precio_frec.agg(['count', 'mean'])\n"
        "resultado = {k: [int(v['count']), round(float(v['mean']), 4)] "
        "for k, v in agg.iterrows()}"
    )
    m_pandas = medir_pandas(codigo)

    def en_spark():
        agg = (
            spark.read.parquet(str(vol["parquet_dir"]))
            .groupBy("variedad")
            .agg(F.count("*").alias("n"), F.avg("precio_frec").alias("media"))
            .collect()
        )
        return {r["variedad"]: [int(r["n"]), round(float(r["media"]), 4)] for r in agg}

    m_spark = medir_spark(en_spark)
    iguales = m_pandas["resultado"] == m_spark["resultado"]

    return [
        {"prueba": "B. agregacion", "variante": "groupBy variedad", "motor": "pandas",
         "filas": vol["filas"], "tamano_mb": tamano_mb(vol["parquet"]),
         "segundos": m_pandas["segundos"], "ram_mb": m_pandas["ram_mb"],
         "resultado": "Correcto" if iguales else "Distinto"},
        {"prueba": "B. agregacion", "variante": "groupBy variedad", "motor": "Spark",
         "filas": vol["filas"], "tamano_mb": tamano_mb(vol["parquet"]),
         "segundos": m_spark["segundos"], "ram_mb": None,
         "resultado": "Correcto" if iguales else "Distinto"},
    ]


def prueba_particionado(spark, vol):
    """C. Filtramos un mes en Parquet sin particionar y particionado por mes."""
    salida, conteos = [], {}
    for variante, ruta in [("sin particionar", vol["parquet_dir"]),
                           ("particionado por mes", vol["parquet_mes"])]:
        consulta = spark.read.parquet(str(ruta)).filter(F.col("mes") == 1)
        poda = "PartitionFilters: [isnotnull(mes" in plan_fisico(consulta)
        m = medir_spark(consulta.count)
        conteos[variante] = m["resultado"]
        salida.append({
            "prueba": "C. filtro un mes", "variante": variante, "motor": "Spark",
            "filas": vol["filas"], "tamano_mb": tamano_mb(ruta),
            "segundos": m["segundos"], "ram_mb": None,
            "resultado": "PartitionFilters" if poda else "lee todo",
        })
    if len(set(conteos.values())) != 1:
        for fila in salida:
            fila["resultado"] += " | conteos distintos"
    return salida


def prueba_shuffle(spark, vol):
    """D. El mismo groupBy con 4, 8 y 16 particiones de shuffle."""
    salida = []
    original = spark.conf.get("spark.sql.shuffle.partitions")
    for n in [4, 8, 16]:
        spark.conf.set("spark.sql.shuffle.partitions", str(n))
        consulta = (
            spark.read.parquet(str(vol["parquet_dir"]))
            .groupBy("estado_origen", "variedad")
            .agg(F.avg("precio_frec").alias("precio"))
        )
        m = medir_spark(lambda: len(consulta.collect()))
        salida.append({
            "prueba": "D. particiones de shuffle", "variante": f"{n} particiones",
            "motor": "Spark", "filas": vol["filas"], "tamano_mb": tamano_mb(vol["parquet"]),
            "segundos": m["segundos"], "ram_mb": None,
            "resultado": f"{m['resultado']} grupos",
        })
    spark.conf.set("spark.sql.shuffle.partitions", original)
    return salida


# --- Figura -----------------------------------------------------------------------

def graficar(resultados):
    """Tiempo contra volumen, en escala log, para A y B."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = [
        ("A. lectura", "CSV", "pandas", "pandas lee CSV", "#c0392b", "-"),
        ("A. lectura", "Parquet", "pandas", "pandas lee Parquet", "#2e86c1", "-"),
        ("B. agregacion", "groupBy variedad", "pandas", "groupBy en pandas", "#c0392b", "--"),
        ("B. agregacion", "groupBy variedad", "Spark", "groupBy en Spark", "#2e86c1", "--"),
    ]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for prueba, variante, motor, etiqueta, color, linea in series:
        d = resultados[(resultados.prueba == prueba) & (resultados.variante == variante)
                       & (resultados.motor == motor)].sort_values("filas")
        ax.plot(d.filas, d.segundos, linea, marker="o", color=color, label=etiqueta)

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Filas (cotizaciones SNIIM replicadas)")
    ax.set_ylabel("Segundos (mediana de 3)")
    ax.set_title("Tiempo contra volumen")
    ax.grid(True, which="major", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)
    fig.tight_layout()
    # SVG: es texto, pesa poco y se ve nitido en GitHub.
    plt.rcParams["svg.fonttype"] = "none"
    fig.savefig(FIGURAS / "desempeno_volumen.svg")
    plt.close(fig)


# --- Punto de entrada -------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description="Comparacion de desempeno")
    parser.add_argument("--factores", type=int, nargs="+", default=[1, 10, 100])
    args = parser.parse_args(argv)

    asegurar_carpetas()
    ruta_base = PROCESSED / "sniim_precios"
    if not ruta_base.exists():
        log.error("No existe data/processed/sniim_precios. "
                  "Primero: python -m src.processing.build_dataset")
        return 1

    spark = crear_spark("DatosMasivos-Benchmark")
    base = spark.read.parquet(str(ruta_base)).select(*COLUMNAS)
    base = intentar_cache(base)
    log.info(f"Base: {base.count():,} cotizaciones | factores {args.factores}")

    filas = []
    volumenes = {}
    for factor in args.factores:
        vol = preparar_volumen(spark, base, factor)
        volumenes[factor] = vol
        log.info(f"x{factor:<4d} | {vol['filas']:>10,} filas | CSV {tamano_mb(vol['csv']):7.1f} MB "
                 f"| Parquet {tamano_mb(vol['parquet']):6.1f} MB")

        filas += prueba_lectura(vol)
        filas += prueba_agregacion(spark, vol)
        filas += prueba_particionado(spark, vol)

    filas += prueba_shuffle(spark, volumenes[max(args.factores)])

    resultados = pd.DataFrame(filas)
    resultados["tamano_mb"] = resultados.tamano_mb.round(2)
    resultados["segundos"] = resultados.segundos.round(3)
    resultados["ram_mb"] = resultados.ram_mb.round(1)
    resultados.to_csv(TABLAS / "desempeno.csv", index=False, encoding="utf-8")
    graficar(resultados)

    for _, f in resultados.iterrows():
        log.info(f"{f.prueba:26s} | {f.variante:22s} | {f.motor:6s} | {f.filas:>10,} filas "
                 f"| {f.segundos:7.3f} s | RAM {f.ram_mb if pd.notna(f.ram_mb) else 'n/d'} "
                 f"| {f.resultado}")
    log.info("Resultados -> reports/tables/desempeno.csv, reports/figures/desempeno_volumen.svg")

    cerrar_spark(spark)
    shutil.rmtree(CARPETA, ignore_errors=True)      # los volumenes replicados pesan; no los dejamos
    return 0


if __name__ == "__main__":
    sys.exit(main())
