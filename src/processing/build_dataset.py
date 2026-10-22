"""
Construccion del dataset: data/raw -> data/interim -> data/processed.

Ejecucion desde la raiz del repositorio:

    python -m src.processing.build_dataset                  # todo lo que tenga crudos
    python -m src.processing.build_dataset --fuente e1      # SIAP + SNIIM
    python -m src.processing.build_dataset --fuente indice  # DENUE + CONAPO + Comtrade

Capas que escribimos (todas en Parquet):

    data/interim/siap_papa/          SIAP unificado y filtrado a papa, como texto
    data/interim/sniim_precios/      registros parseados del HTML, como texto
    data/processed/siap_produccion/  SIAP tipado y limpio, particionado por anio
    data/processed/sniim_precios/    SNIIM tipado y limpio, particionado por anio y mes
    data/processed/canalizacion/     indice de canalizacion por anio y estado
    data/interim/denue/              DENUE unificado entre versiones, como texto
    data/processed/denue_establecimientos/  DENUE con categoria y cadena (version=)
    data/processed/metropolis*/      catalogo CONAPO de zonas metropolitanas
    data/processed/importaciones/    Comtrade: mes x pais, valor, kilos, usd/kg
    data/processed/resumen_build.json  filas por capa, descartes y tiempos

Pipeline:
    raw (CSV/HTML) -> interim (Parquet texto) -> processed (Parquet tipado)
"""

import argparse
import json
import sys
import time
from datetime import datetime

from src.processing import canalizacion, comtrade, denue, metropolis, siap, sniim
from src.utils.config import cargar_config
from src.utils.registro import configurar_registro
from src.utils.rutas import INTERIM, PROCESSED, RAW, asegurar_carpetas
from src.utils.spark import cerrar_spark, crear_spark

log = configurar_registro("processing")

SALIDAS = {
    "interim_siap": INTERIM / "siap_papa",
    "interim_sniim": INTERIM / "sniim_precios",
    "siap": PROCESSED / "siap_produccion",
    "sniim": PROCESSED / "sniim_precios",
    "canalizacion": PROCESSED / "canalizacion",
    # Indice de inversion
    "interim_denue": INTERIM / "denue",
    "denue": PROCESSED / "denue_establecimientos",
    "metropolis_municipios": PROCESSED / "metropolis_municipios",
    "metropolis": PROCESSED / "metropolis",
    "importaciones": PROCESSED / "importaciones",
}


def escribir(df, ruta, particiones=None, archivos=None):
    """Escribimos Parquet en modo overwrite: el pipeline es idempotente.

    archivos: si lo damos, hacemos coalesce para no dejar decenas de archivos
    diminutos (la tabla de canalizacion cabe en uno).
    """
    if archivos:
        df = df.coalesce(archivos)
    escritor = df.write.mode("overwrite")
    if particiones:
        escritor = escritor.partitionBy(*particiones)
    escritor.parquet(str(ruta))


def construir(spark, config):
    """SIAP y SNIIM (entregables 1 y 2): las dos etapas. Devolvemos el resumen."""
    resumen = {"etapas": {}}

    # --- raw -> interim --------------------------------------------------------
    inicio = time.perf_counter()

    rutas_siap = sorted((RAW / "siap").glob("siap_cierre_*.csv"))
    interim_siap = siap.construir_interim(spark, rutas_siap, config["siap"]["cultivo"])
    escribir(interim_siap, SALIDAS["interim_siap"], particiones=["Anio"])

    interim_sniim = sniim.construir_interim(spark, RAW / "sniim",
                                            config["sniim"]["variedades"])
    escribir(interim_sniim, SALIDAS["interim_sniim"], particiones=["variedad"])

    resumen["etapas"]["raw_a_interim"] = {
        "archivos_siap": len(rutas_siap),
        "archivos_sniim": len(list((RAW / "sniim").glob("*/*.html"))),
        "segundos": round(time.perf_counter() - inicio, 2),
    }
    log.info(f"raw -> interim | {len(rutas_siap)} CSV SIAP, "
             f"{resumen['etapas']['raw_a_interim']['archivos_sniim']} HTML SNIIM "
             f"| {resumen['etapas']['raw_a_interim']['segundos']} s")

    # --- interim -> processed --------------------------------------------------
    # Releemos interim desde disco: cada etapa arranca de la capa anterior y no
    # de un plan en memoria, asi podemos rehacer solo processed si cambia la limpieza.
    inicio = time.perf_counter()

    df_siap, metricas_siap = siap.construir_processed(
        spark.read.parquet(str(SALIDAS["interim_siap"]))
    )
    escribir(df_siap, SALIDAS["siap"], particiones=["anio"])

    df_sniim, metricas_sniim = sniim.construir_processed(
        spark.read.parquet(str(SALIDAS["interim_sniim"]))
    )
    escribir(df_sniim, SALIDAS["sniim"], particiones=["anio", "mes"])

    df_canal = canalizacion.construir_canalizacion(
        spark.read.parquet(str(SALIDAS["siap"])),
        spark.read.parquet(str(SALIDAS["sniim"])),
    )
    escribir(df_canal, SALIDAS["canalizacion"], archivos=1)

    resumen["etapas"]["interim_a_processed"] = {
        "siap": metricas_siap,
        "sniim": metricas_sniim,
        "canalizacion_filas": spark.read.parquet(str(SALIDAS["canalizacion"])).count(),
        "segundos": round(time.perf_counter() - inicio, 2),
    }

    for nombre, m in [("SIAP ", metricas_siap), ("SNIIM", metricas_sniim)]:
        log.info(f"{nombre} | interim {m['filas_interim']:6,} -> processed "
                 f"{m['filas_processed']:6,} | nulos llave {m['filas_nulos_llave']} "
                 f"| duplicados {m['filas_duplicadas']}")

    return resumen


def construir_indice(spark, config):
    """DENUE, CONAPO y Comtrade: las fuentes del indice de inversion.

    Cada fuente se procesa si tiene crudos; si no, la saltamos con un aviso
    (por ejemplo, al correr solo con --muestra).
    """
    resumen = {}

    # CONAPO: catalogo de metropolis (chico, va directo a processed).
    carpeta = RAW / "metropolis"
    if (carpeta / "municipios.csv").exists() and (carpeta / "caracteristicas.csv").exists():
        escribir(metropolis.construir_municipios(spark, carpeta / "municipios.csv"),
                 SALIDAS["metropolis_municipios"], archivos=1)
        escribir(metropolis.construir_metropolis(spark, carpeta / "caracteristicas.csv"),
                 SALIDAS["metropolis"], archivos=1)
        resumen["metropolis"] = {
            "municipios": spark.read.parquet(str(SALIDAS["metropolis_municipios"])).count(),
            "metropolis": spark.read.parquet(str(SALIDAS["metropolis"])).count(),
        }
        log.info(f"CONAPO | {resumen['metropolis']['metropolis']} metropolis, "
                 f"{resumen['metropolis']['municipios']} municipios")
    else:
        log.warning("CONAPO | sin crudos en raw/metropolis/, lo saltamos")

    # Comtrade: un JSON por mes.
    if list((RAW / "comtrade").glob("*.json")):
        importaciones = comtrade.construir_processed(spark, RAW / "comtrade")
        escribir(importaciones, SALIDAS["importaciones"], archivos=1)
        resumen["comtrade_filas"] = spark.read.parquet(str(SALIDAS["importaciones"])).count()
        log.info(f"Comtrade | {resumen['comtrade_filas']:,} filas mes-pais")
    else:
        log.warning("Comtrade | sin crudos en raw/comtrade/, lo saltamos")

    # DENUE: la fuente grande. raw (zip) -> interim (texto) -> processed.
    if list((RAW / "denue").glob("*/*.zip")):
        inicio = time.perf_counter()
        interim = denue.construir_interim(spark, RAW / "denue",
                                          denue.ruta_csv_staging(INTERIM),
                                          config["denue"]["versiones"])
        escribir(interim, SALIDAS["interim_denue"], particiones=["version"])

        df, metricas = denue.construir_processed(
            spark.read.parquet(str(SALIDAS["interim_denue"])), config["denue"])
        escribir(df, SALIDAS["denue"], particiones=["version"])
        metricas["segundos"] = round(time.perf_counter() - inicio, 2)
        resumen["denue"] = metricas
        log.info(f"DENUE | interim {metricas['filas_interim']:,} -> processed "
                 f"{metricas['filas_processed']:,} | sin llave {metricas['filas_sin_llave']} "
                 f"| duplicados {metricas['filas_duplicadas']} | {metricas['segundos']} s")
    else:
        log.warning("DENUE | sin crudos en raw/denue/, lo saltamos")

    return resumen


def main(argv=None):
    parser = argparse.ArgumentParser(description="Construccion del dataset")
    parser.add_argument("--fuente", choices=["e1", "indice", "todas"], default="todas",
                        help="e1 = SIAP + SNIIM; indice = DENUE + CONAPO + Comtrade")
    # Desde un notebook llamamos main() sin argumentos: no leemos sys.argv,
    # que ahi trae los argumentos del kernel.
    args = parser.parse_args(argv or [])

    asegurar_carpetas()
    config = cargar_config()
    proc = config["procesamiento"]

    spark = crear_spark("DatosMasivos-BuildDataset",
                        particiones_shuffle=proc["particiones_shuffle"],
                        memoria=proc["memoria_driver"])
    inicio = time.perf_counter()

    resumen = {"inicio": datetime.now().isoformat(timespec="seconds"), "etapas": {}}
    try:
        if args.fuente in ("e1", "todas"):
            hay_e1 = (list((RAW / "siap").glob("*.csv"))
                      and list((RAW / "sniim").glob("*/*.html")))
            if hay_e1 or args.fuente == "e1":
                resumen.update(construir(spark, config))
            else:
                log.warning("SIAP/SNIIM | sin crudos, los saltamos")
        if args.fuente in ("indice", "todas"):
            resumen["indice"] = construir_indice(spark, config)
    except FileNotFoundError as error:
        log.error(str(error))
        return 1
    finally:
        segundos = round(time.perf_counter() - inicio, 2)

    resumen["fin"] = datetime.now().isoformat(timespec="seconds")

    resumen["segundos_totales"] = segundos
    # Si corremos solo una parte (--fuente indice), conservamos lo que la
    # otra parte dejo en el resumen.
    ruta_resumen = PROCESSED / "resumen_build.json"
    if ruta_resumen.exists() and args.fuente != "todas":
        anterior = json.loads(ruta_resumen.read_text(encoding="utf-8"))
        anterior.update({k: v for k, v in resumen.items() if v != {}})
        resumen = anterior
    ruta_resumen.write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Dataset construido en {segundos} s -> {PROCESSED}")
    cerrar_spark(spark)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
