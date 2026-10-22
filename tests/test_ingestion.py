"""Pruebas de la adquisicion: parseo, cache, errores y manifiesto. Sin internet."""

import json

import pandas as pd
import pytest

from src.ingestion import download_data, siap_extractor, sniim_extractor
from tests.conftest import FILAS_ALPHA, html_sniim


@pytest.fixture
def raw_temporal(tmp_path, monkeypatch):
    """Redirigimos data/raw a una carpeta temporal para no tocar el repositorio."""
    monkeypatch.setattr(download_data, "RAW", tmp_path)
    monkeypatch.setattr(download_data, "MANIFIESTO", tmp_path / "manifiesto.json")
    monkeypatch.setattr(download_data.time, "sleep", lambda s: None)
    return tmp_path


def config_minima(anios_siap=(2013,), anios_sniim=(2025,)):
    return {
        "siap": {"anios": list(anios_siap), "pausa": 0, "timeout": 1},
        "sniim": {"anios": list(anios_sniim), "variedades": {740: "Alpha"},
                  "pausa": 0, "timeout": 1, "intentos": 1},
    }


# --- Parseo del HTML -----------------------------------------------------------

def test_parsear_html_toma_solo_filas_de_datos():
    registros = sniim_extractor.parsear_html(html_sniim(FILAS_ALPHA), "Alpha")

    # 3 filas de datos; los dos encabezados y la tabla de diseno se ignoran.
    assert len(registros) == 3
    assert registros[0]["precio_frec"] == "20.00"
    assert registros[0]["variedad"] == "Alpha"
    assert set(registros[0]) == set(sniim_extractor.CAMPOS) | {"variedad"}


def test_parsear_html_sin_tabla_devuelve_lista_vacia():
    assert sniim_extractor.parsear_html("<html><body>sin datos</body></html>", "Alpha") == []


# --- Normalizacion de SIAP ------------------------------------------------------

def test_normalizar_siap_unifica_encabezados_y_tipos():
    df = pd.DataFrame({
        "Anio": ["2017", "2017"],
        "Nomestado ": ["Sinaloa", "Sinaloa"],          # espacio en el encabezado
        "Nomcultivo Sin Um": ["Papa", "Maíz grano"],
        "Precio": ["6,000", "4000"],
        "Volumenproduccion": ["1,500.5", "10"],
    })
    salida = siap_extractor.normalizar(df, cultivo="papa")

    assert len(salida) == 1
    assert "Nomcultivo" in salida.columns and "Preciomediorural" in salida.columns
    assert salida.loc[0, "Preciomediorural"] == 6000
    assert salida.loc[0, "Volumenproduccion"] == 1500.5


# --- Cache, manifiesto y errores ------------------------------------------------

def test_siap_guarda_crudo_y_manifiesto(raw_temporal, monkeypatch):
    monkeypatch.setattr(siap_extractor, "descargar_crudo", lambda anio, timeout: b"Anio\n2013\n")
    manifiesto = {"archivos": {}}

    errores = download_data.descargar_siap(config_minima(), manifiesto)

    archivo = raw_temporal / "siap" / "siap_cierre_2013.csv"
    assert errores == 0
    assert archivo.read_bytes() == b"Anio\n2013\n"          # copia cruda, sin tocar
    entrada = manifiesto["archivos"]["siap/siap_cierre_2013.csv"]
    assert entrada["estado"] == "ok" and len(entrada["sha256"]) == 64


def test_cache_evita_descargas_innecesarias(raw_temporal, monkeypatch):
    llamadas = []

    def falsa(anio, timeout):
        llamadas.append(anio)
        return b"datos"

    monkeypatch.setattr(siap_extractor, "descargar_crudo", falsa)
    download_data.descargar_siap(config_minima(), {"archivos": {}})
    download_data.descargar_siap(config_minima(), {"archivos": {}})

    assert llamadas == [2013]                                # la segunda vez sale de cache


def test_forzar_vuelve_a_descargar(raw_temporal, monkeypatch):
    llamadas = []
    monkeypatch.setattr(siap_extractor, "descargar_crudo",
                        lambda anio, timeout: llamadas.append(anio) or b"datos")
    download_data.descargar_siap(config_minima(), {"archivos": {}})
    download_data.descargar_siap(config_minima(), {"archivos": {}}, forzar=True)

    assert llamadas == [2013, 2013]


def test_error_se_registra_y_no_detiene_la_corrida(raw_temporal, monkeypatch):
    def falla_en_2013(anio, timeout):
        if anio == 2013:
            raise ConnectionError("portal caido")
        return b"ok"

    monkeypatch.setattr(siap_extractor, "descargar_crudo", falla_en_2013)
    manifiesto = {"archivos": {}}

    errores = download_data.descargar_siap(config_minima(anios_siap=(2013, 2014)), manifiesto)

    assert errores == 1
    assert manifiesto["archivos"]["siap/siap_cierre_2013.csv"]["estado"] == "error"
    assert (raw_temporal / "siap" / "siap_cierre_2014.csv").exists()   # siguio con 2014
    assert not (raw_temporal / "siap" / "siap_cierre_2013.csv").exists()


def test_sniim_marca_particiones_truncadas(raw_temporal, monkeypatch):
    fila = FILAS_ALPHA[0]
    html_lleno = html_sniim([fila] * sniim_extractor.LIMITE)
    monkeypatch.setattr(sniim_extractor, "descargar_pagina", lambda *a, **k: html_lleno)
    monkeypatch.setattr(download_data, "meses_disponibles", lambda anio: [3])
    manifiesto = {"archivos": {}}

    download_data.descargar_sniim(config_minima(), manifiesto)

    entrada = manifiesto["archivos"]["sniim/alpha/sniim_740_2025-03.html"]
    assert entrada["filas"] == sniim_extractor.LIMITE
    assert entrada["truncado"] is True


def test_manifiesto_se_guarda_como_json(raw_temporal):
    download_data.guardar_manifiesto({"archivos": {"x": {"estado": "ok"}}})
    contenido = json.loads((raw_temporal / "manifiesto.json").read_text())
    assert contenido["archivos"]["x"]["estado"] == "ok" and "actualizado" in contenido


# --- Muestra sin internet -------------------------------------------------------

def test_muestra_reconstruye_formato_crudo(tmp_path, monkeypatch):
    from src.ingestion import muestra

    monkeypatch.setattr(muestra, "RAW", tmp_path)
    conteo = muestra.construir_muestra()

    assert conteo["siap"] == 13 and conteo["sniim"] > 0

    # El CSV de 2017 debe traer el encabezado viejo, en latin-1.
    encabezado = (tmp_path / "siap" / "siap_cierre_2017.csv").read_bytes().decode("latin-1")
    assert "Nomcultivo Sin Um" in encabezado.splitlines()[0]

    # Y el HTML se parsea igual que una pagina real del SNIIM.
    pagina = next((tmp_path / "sniim" / "alpha").glob("*.html")).read_text("utf-8")
    assert len(sniim_extractor.parsear_html(pagina, "Alpha")) > 0


def test_muestra_no_sobrescribe_una_descarga_real(raw_temporal, monkeypatch):
    llamadas = []
    monkeypatch.setattr("src.ingestion.muestra.construir_muestra",
                        lambda: llamadas.append(1) or {"siap": 13, "sniim": 15})
    download_data.guardar_manifiesto({"archivos": {"siap/x.csv": {"estado": "ok"}}})

    assert download_data.main(["--muestra"]) == 1
    assert llamadas == []                                   # no tocamos data/raw

    assert download_data.main(["--muestra", "--forzar"]) == 0
    assert llamadas == [1]
    assert not (raw_temporal / "manifiesto.json").exists()  # ya no corresponde


def test_muestra_detecta_csv_real_sin_manifiesto(raw_temporal):
    carpeta = raw_temporal / "siap"
    carpeta.mkdir()
    (carpeta / "siap_cierre_2013.csv").write_bytes(b"x" * (2 * 1024**2))

    assert download_data.hay_descarga_real() is True
