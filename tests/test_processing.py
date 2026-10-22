"""Pruebas del procesamiento con Spark sobre datos crudos diminutos."""

import pytest

from src.processing import canalizacion, siap, sniim
from tests.conftest import VARIEDADES


@pytest.fixture
def siap_processed(spark, raw_siap):
    interim = siap.construir_interim(spark, raw_siap)
    df, metricas = siap.construir_processed(interim)
    return df, metricas


@pytest.fixture
def sniim_processed(spark, raw_sniim):
    interim = sniim.construir_interim(spark, raw_sniim, VARIEDADES)
    df, metricas = sniim.construir_processed(interim)
    return df, metricas


# --- SIAP -----------------------------------------------------------------------

def test_siap_interim_une_anios_con_encabezados_distintos(spark, raw_siap):
    interim = siap.construir_interim(spark, raw_siap)

    # 2013 trae "Precio"; 2017 trae "Nomcultivo Sin Um". Ambos quedan unificados.
    assert "Preciomediorural" in interim.columns and "Nomcultivo" in interim.columns
    precios = {r["Anio"]: r["Preciomediorural"] for r in interim.collect()}
    assert precios["2017"] == "6000"


def test_siap_interim_filtra_solo_papa(spark, raw_siap):
    cultivos = {r[0] for r in siap.construir_interim(spark, raw_siap).select("Nomcultivo").collect()}
    assert cultivos == {"Papa"}


def test_siap_sin_archivos_crudos_explica_que_correr(spark):
    with pytest.raises(FileNotFoundError, match="download_data"):
        siap.construir_interim(spark, [])


def test_siap_processed_tipos_y_comas_de_miles(siap_processed):
    df, _ = siap_processed
    tipos = dict(df.dtypes)
    assert tipos["anio"] == "int" and tipos["produccion_ton"] == "double"

    sonora = df.filter("estado = 'Sonora'").first()
    assert sonora["produccion_ton"] == 3000.0            # "3,000" -> 3000.0


def test_siap_processed_elimina_duplicados(siap_processed):
    df, metricas = siap_processed
    assert metricas["filas_duplicadas"] == 1
    assert df.count() == 3                                # Sonora, CDMX, Sinaloa


def test_siap_estandariza_la_capital(siap_processed):
    df, _ = siap_processed
    estados = {r["estado"] for r in df.select("estado").collect()}
    assert "Ciudad de México" in estados and "Distrito Federal" not in estados


def test_siap_variables_derivadas(siap_processed):
    df, _ = siap_processed
    cdmx = df.filter("estado = 'Ciudad de México'").first()
    assert cdmx["pct_siniestrada"] == pytest.approx(20.0)     # 2 de 10 ha
    assert cdmx["valor_por_ton"] == pytest.approx(7000.0)


# --- SNIIM ----------------------------------------------------------------------

def test_sniim_interim_recupera_variedad_y_trazabilidad(spark, raw_sniim):
    interim = sniim.construir_interim(spark, raw_sniim, VARIEDADES)
    filas = interim.collect()

    assert {r["variedad"] for r in filas} == {"Alpha", "Rouset"}
    assert all(r["archivo"].startswith("sniim_") for r in filas)


def test_sniim_processed_tipos_y_duplicados(sniim_processed):
    df, metricas = sniim_processed
    tipos = dict(df.dtypes)

    assert tipos["fecha"] == "date" and tipos["precio_frec"] == "double"
    assert metricas["filas_duplicadas"] == 1
    assert df.count() == 3


def test_sniim_separa_estado_destino_y_mercado(sniim_processed):
    df, _ = sniim_processed
    tijuana = df.filter("variedad = 'Rouset'").first()

    assert tijuana["estado_destino"] == "Baja California"      # sin el espacio antes de ":"
    assert tijuana["mercado"] == "Central de Abasto INDIA, Tijuana"


def test_sniim_marca_importacion_sin_estado_de_origen(sniim_processed):
    df, _ = sniim_processed
    rouset = df.filter("variedad = 'Rouset'").first()
    assert rouset["es_importacion"] is True and rouset["estado_origen"] is None


def test_sniim_df_y_distrito_federal_son_la_misma_capital(sniim_processed):
    df, _ = sniim_processed
    destinos = {r["estado_destino"] for r in df.select("estado_destino").collect()}
    origenes = {r["estado_origen"] for r in df.select("estado_origen").collect()}
    assert "Ciudad de México" in destinos and "Ciudad de México" in origenes


# --- Cruce ----------------------------------------------------------------------

def test_canalizacion_participaciones_suman_100(spark, siap_processed, sniim_processed):
    # Llevamos SIAP a 2025 para que haya anio comun con SNIIM.
    siap_2025 = siap_processed[0].withColumn("anio", siap_processed[0]["anio"] * 0 + 2025)
    canal = canalizacion.construir_canalizacion(siap_2025, sniim_processed[0]).collect()

    assert sum(r["pct_nacional"] for r in canal) == pytest.approx(100)
    assert sum(r["pct_mayoreo"] for r in canal) == pytest.approx(100)

    # La importacion no cuenta: los 2 registros nacionales son Sinaloa y CDMX.
    por_estado = {r["estado"]: r for r in canal}
    assert por_estado["Sonora"]["cotizaciones"] == 0
    assert por_estado["Sonora"]["canalizacion"] > 0


# --- Databricks -------------------------------------------------------------

def test_parsear_archivo_funciona_sin_spark():
    """La funcion de la UDF debe ser autosuficiente: en serverless corre sola en el worker."""
    from tests.conftest import FILAS_ALPHA, html_sniim

    filas = sniim.parsear_archivo("dbfs:/Volumes/x/raw/sniim/alpha/sniim_740_2025-03.html",
                                  html_sniim(FILAS_ALPHA))
    assert len(filas) == 3
    assert filas[0]["producto_id"] == "740" and filas[0]["archivo"] == "sniim_740_2025-03.html"
    assert sniim.parsear_archivo("ruta/que/no/coincide.html", "<html></html>") == []


def test_las_capas_se_pueden_mover_al_data_lake(tmp_path):
    """Con DATOS_MASIVOS_DATA las capas viven fuera del repo (el Volume en Databricks)."""
    import os
    import subprocess
    import sys

    entorno = {**os.environ, "DATOS_MASIVOS_DATA": str(tmp_path / "datalake"),
               "DATABRICKS_RUNTIME_VERSION": "client.1"}
    salida = subprocess.run(
        [sys.executable, "-c",
         "from src.utils import rutas; print(rutas.RAW); print(rutas.EN_DATABRICKS); "
         "print(rutas.LOGS)"],
        capture_output=True, text=True, check=True, env=entorno,
    ).stdout.split()

    assert salida[0] == str(tmp_path / "datalake" / "raw")
    assert salida[1] == "True"
    assert salida[2].startswith("/tmp")
