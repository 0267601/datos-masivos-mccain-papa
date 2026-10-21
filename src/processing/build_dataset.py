"""
Construccion del dataset: data/raw -> data/interim -> data/processed.

Ejecucion desde la raiz del repositorio:

    python -m src.processing.build_dataset

Capas que escribimos (todas en Parquet):

    data/interim/siap_papa/          SIAP unificado y filtrado a papa, como texto
    data/interim/sniim_precios/      registros parseados del HTML, como texto
    data/processed/siap_produccion/  SIAP tipado y limpio, particionado por anio
    data/processed/sniim_precios/    SNIIM tipado y limpio, particionado por anio y mes
    data/processed/canalizacion/     indice de canalizacion por anio y estado
    data/processed/resumen_build.json  filas por capa, descartes y tiempos

Pipeline:
    raw (CSV/HTML) -> interim (Parquet texto) -> processed (Parquet tipado)
"""

import json
import sys
import time
from datetime import datetime

from src.processing import canalizacion, siap, sniim
from src.utils.config import cargar_config
from src.utils.registro import configurar_registro
from src.utils.rutas import INTERIM, PROCESSED, RAW, asegurar_carpetas
from src.utils.spark import crear_spark

log = configurar_registro("processing")

SALIDAS = {
    "interim_siap": INTERIM / "siap_papa",
    "interim_sniim": INTERIM / "sniim_precios",
    "siap": PROCESSED / "siap_produccion",
    "sniim": PROCESSED / "sniim_precios",
    "canalizacion": PROCESSED / "canalizacion",
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
    """Corremos las dos etapas y devolvemos el resumen."""
    resumen = {"inicio": datetime.now().isoformat(timespec="seconds"), "etapas": {}}

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

    resumen["fin"] = datetime.now().isoformat(timespec="seconds")
    return resumen


def main():
    asegurar_carpetas()
    config = cargar_config()
    proc = config["procesamiento"]

    spark = crear_spark("DatosMasivos-BuildDataset",
                        particiones_shuffle=proc["particiones_shuffle"],
                        memoria=proc["memoria_driver"])
    inicio = time.perf_counter()

    try:
        resumen = construir(spark, config)
    except FileNotFoundError as error:
        log.error(str(error))
        return 1
    finally:
        segundos = round(time.perf_counter() - inicio, 2)

    resumen["segundos_totales"] = segundos
    (PROCESSED / "resumen_build.json").write_text(
        json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info(f"Dataset construido en {segundos} s -> data/processed/")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
