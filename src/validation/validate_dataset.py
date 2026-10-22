"""
Validacion del dataset procesado.

Ejecucion desde la raiz del repositorio (despues de build_dataset):

    python -m src.validation.validate_dataset

Corremos las reglas de src/validation/reglas.py (entrega 1: SIAP y SNIIM) y
de src/validation/reglas_indice.py (indice de inversion: DENUE, CONAPO,
Comtrade) sobre data/processed/. Validamos solo los grupos que ya existen, asi
podemos correr cada parte por separado. Escribimos el resultado en:

    reports/tables/validacion.csv   una fila por regla
    reports/tables/validacion.md    la misma tabla en markdown, para docs/validation.md

Regresamos codigo de salida 1 si alguna regla da FALLA. Los AVISO no detienen
el pipeline, pero quedan documentados.
"""

import json
import sys
from datetime import date

import pandas as pd

from src.ingestion.download_data import MANIFIESTO
from src.utils.config import cargar_config
from src.utils.registro import configurar_registro
from src.utils.rutas import PROCESSED, TABLAS, asegurar_carpetas
from src.utils.spark import cerrar_spark, crear_spark, intentar_cache
from src.validation import reglas as r
from src.validation import reglas_indice as ri

log = configurar_registro("validation")

COLUMNAS_SIAP = {
    "anio": "int", "estado": "string", "estado_llave": "string", "municipio": "string",
    "ciclo": "string", "modalidad": "string",
    "sup_sembrada_ha": "double", "sup_cosechada_ha": "double", "sup_siniestrada_ha": "double",
    "produccion_ton": "double", "rendimiento_ton_ha": "double",
    "precio_medio_rural": "double", "valor_produccion": "double",
}
COLUMNAS_SNIIM = {
    "fecha": "date", "anio": "int", "mes": "int", "variedad": "string",
    "presentacion": "string", "origen": "string", "destino": "string",
    "precio_min": "double", "precio_max": "double", "precio_frec": "double",
    "es_importacion": "boolean", "estado_origen_llave": "string",
}
COLUMNAS_CANAL = {
    "anio": "int", "estado": "string", "produccion_ton": "double",
    "pct_nacional": "double", "pct_mayoreo": "double", "canalizacion": "double",
}

LLAVE_SIAP = ["anio", "estado", "ddr", "cader", "municipio", "ciclo", "modalidad"]
LLAVE_SNIIM = ["fecha", "variedad", "origen", "destino", "presentacion"]


def correr_reglas(siap, sniim, canal, config, manifiesto=None, hoy=None):
    """Devolvemos la lista de resultados de todas las reglas."""
    hoy = hoy or date.today()
    v = config["validacion"]
    res = []

    # 1. Columnas — si faltan, detenemos todo con ValueError.
    for df, cols, tabla in [(siap, COLUMNAS_SIAP, "siap"), (sniim, COLUMNAS_SNIIM, "sniim"),
                            (canal, COLUMNAS_CANAL, "canalizacion")]:
        r.exigir_columnas(df, cols)
        res.append(r.validar_columnas(df, cols, tabla))

    # 2. Tipos
    res.append(r.validar_tipos(siap, COLUMNAS_SIAP, "siap"))
    res.append(r.validar_tipos(sniim, COLUMNAS_SNIIM, "sniim"))
    res.append(r.validar_tipos(canal, COLUMNAS_CANAL, "canalizacion"))

    # 3. Nulos
    res += r.validar_nulos(siap, {"estado": 0, "produccion_ton": 0, "sup_sembrada_ha": 1,
                                  "rendimiento_ton_ha": 5, "precio_medio_rural": 5}, "siap")
    res += r.validar_nulos(sniim, {"fecha": 0, "precio_frec": 0, "precio_min": 1,
                                   "precio_max": 1, "origen": 0}, "sniim")

    # 4. Duplicados
    res.append(r.validar_duplicados(siap, LLAVE_SIAP, "siap"))
    res.append(r.validar_duplicados(sniim, LLAVE_SNIIM, "sniim"))
    res.append(r.validar_duplicados(canal, ["anio", "estado_llave"], "canalizacion"))

    # 5. Rangos
    res.append(r.validar_rango(siap, "produccion_ton", 0, float("inf"), "siap"))
    res.append(r.validar_rango(siap, "sup_sembrada_ha", 0, float("inf"), "siap"))
    res.append(r.validar_rango(siap, "rendimiento_ton_ha", 0, v["rendimiento_max_ton_ha"], "siap"))
    res.append(r.validar_orden(siap, "sup_cosechada_ha", "sup_sembrada_ha", "siap"))
    res.append(r.validar_identidad_produccion(siap, "siap"))
    pmin, pmax = v["precio_kg_rango"]
    res.append(r.validar_rango(sniim, "precio_frec", pmin, pmax, "sniim", nivel=r.AVISO))
    res.append(r.validar_orden(sniim, "precio_min", "precio_frec", "sniim"))
    res.append(r.validar_orden(sniim, "precio_frec", "precio_max", "sniim"))

    # 6. Consistencia temporal
    res.append(r.validar_anios(siap, "anio", config["siap"]["anios"], "siap"))
    res.append(r.validar_anios(sniim, "anio", config["sniim"]["anios"], "sniim"))
    res.append(r.validar_fechas(sniim, "sniim", hoy))
    res.append(r.validar_cobertura_mensual(sniim, "sniim"))

    # 7. Consistencia entre tablas
    res.append(r.validar_estados_cruce(siap, sniim))
    res.append(r.validar_porcentajes(canal))

    # 8. Volumen
    fmin, fmax = v["siap_filas_por_anio"]
    res.append(r.validar_volumen_por_anio(siap, "anio", fmin, fmax, "siap"))
    res.append(r.validar_volumen_minimo(sniim, v["sniim_filas_minimas"], "sniim"))
    res.append(r.validar_truncamiento(manifiesto))

    return res


def a_markdown(tabla):
    """Tabla markdown sin depender de tabulate."""
    columnas = list(tabla.columns)
    lineas = ["| " + " | ".join(columnas) + " |",
              "|" + "---|" * len(columnas)]
    for _, fila in tabla.iterrows():
        lineas.append("| " + " | ".join(str(fila[c]).replace("|", "/") for c in columnas) + " |")
    return "\n".join(lineas) + "\n"


def reglas_entrega1(spark, config):
    """Reglas de SIAP, SNIIM y canalizacion (si existen)."""
    rutas = {n: PROCESSED / n for n in ["siap_produccion", "sniim_precios", "canalizacion"]}
    if not all(p.exists() for p in rutas.values()):
        log.warning("Sin tablas de SIAP/SNIIM: saltamos sus reglas")
        return []

    # Leemos una vez y reusamos: las reglas hacen muchos count().
    siap = intentar_cache(spark.read.parquet(str(rutas["siap_produccion"])))
    sniim = intentar_cache(spark.read.parquet(str(rutas["sniim_precios"])))
    canal = spark.read.parquet(str(rutas["canalizacion"]))

    manifiesto = None
    if MANIFIESTO.exists():
        manifiesto = json.loads(MANIFIESTO.read_text(encoding="utf-8"))
    return correr_reglas(siap, sniim, canal, config, manifiesto)


def reglas_indice(spark, config):
    """Reglas del indice de inversion: cada tabla solo si existe."""
    def leer(nombre):
        ruta = PROCESSED / nombre
        if ruta.exists():
            return spark.read.parquet(str(ruta))
        log.warning(f"Sin {nombre}: saltamos sus reglas")
        return None

    res = []
    denue = leer("denue_establecimientos")
    if denue is not None:
        res += ri.reglas_denue(intentar_cache(denue), config)
    municipios, metropolis = leer("metropolis_municipios"), leer("metropolis")
    if municipios is not None and metropolis is not None:
        res += ri.reglas_metropolis(municipios, metropolis)
    importaciones = leer("importaciones")
    if importaciones is not None:
        res += ri.reglas_importaciones(importaciones, config)
    indice = leer("indice_inversion")
    if indice is not None and metropolis is not None:
        res += ri.reglas_indice(indice, metropolis, config)
    return res


def main():
    asegurar_carpetas()
    config = cargar_config()
    spark = crear_spark("DatosMasivos-Validacion")

    filas = reglas_entrega1(spark, config) + reglas_indice(spark, config)
    if not filas:
        log.error("No hay tablas en data/processed. Primero: python -m src.processing.build_dataset")
        cerrar_spark(spark)
        return 1
    resultados = pd.DataFrame(filas)

    resultados.to_csv(TABLAS / "validacion.csv", index=False, encoding="utf-8")
    (TABLAS / "validacion.md").write_text(a_markdown(resultados), encoding="utf-8")

    for _, fila in resultados.iterrows():
        nivel = {"OK": log.info, "AVISO": log.warning, "FALLA": log.error}[fila.estado]
        nivel(f"{fila.estado:5s} | {fila.tabla:12s} | {fila.regla:40s} | {fila.detalle}")

    conteo = resultados.estado.value_counts().to_dict()
    log.info(f"Reglas: {len(resultados)} | {conteo} -> {TABLAS / 'validacion.csv'}")
    cerrar_spark(spark)

    return 1 if conteo.get("FALLA", 0) else 0


if __name__ == "__main__":
    sys.exit(main())
