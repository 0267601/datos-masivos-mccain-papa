"""
Pruebas del indice de inversion: DENUE, CONAPO, Comtrade, indice y reglas.

Armamos zip diminutos con los dos formatos reales del DENUE:
    2015       CSV en la raiz del zip, UTF-8 con BOM, encabezados largos
    2019-2026  CSV en conjunto_de_datos/, Latin-1, encabezados cortos,
               con diccionario de datos al lado (que no debemos leer)
"""

import json
import zipfile
from datetime import date

import pytest
from pyspark.sql import Row

from src.analysis import indice_inversion as ii
from src.ingestion import comtrade_extractor, denue_extractor
from src.processing import comtrade, denue, metropolis
from src.utils.config import cargar_config
from src.validation import reglas_indice as ri

CONFIG = cargar_config()

DENUE_2015 = (
    "﻿\"ID\",\"Nombre de la Unidad Económica\",\"Razón social\","
    "\"Código de la clase de actividad SCIAN\",\"Nombre de clase de la actividad\","
    "\"Descripción estrato personal ocupado\",\"Clave entidad\",\"Entidad federativa\","
    "\"Clave municipio\",\"Municipio\",\"Latitud\",\"Longitud\","
    "\"Fecha de incorporación al DENUE\"\n"
    "\"1\",\"MC DONALDS SATELITE\",\"\",\"722517\","
    "\"Restaurantes con servicio de preparación de pizzas, hamburguesas, hot dogs y pollos rostizados para llevar\","
    "\"11 a 30 personas\",\"15\",\"México\",\"57\",\"Naucalpan de Juárez\",\"19.51\",\"-99.23\","
    "\"JULIO 2010\"\n"
    "\"2\",\"LA FONDA DE DOÑA MARÍA\",\"\",\"722511\","
    "\"Restaurantes con servicio de preparación de alimentos a la carta o de comida corrida\","
    "\"0 a 5 personas\",\"9\",\"Ciudad de México\",\"15\",\"Cuauhtémoc\",\"19.43\",\"-99.14\","
    "\"JULIO 2010\"\n"
)
DENUE_2019 = (
    "id,nom_estab,raz_social,codigo_act,nombre_act,per_ocu,cve_ent,entidad,cve_mun,municipio,"
    "latitud,longitud,fecha_alta\n"
    "10,MCDONALD'S SATELITE,ARCOS SA,722517,"
    "\"Restaurantes con servicio de preparación de pizzas, hamburguesas, hot dogs y pollos rostizados para llevar\","
    "11 a 30 personas,15,México,057,Naucalpan de Juárez,19.51,-99.23,2010-07\n"
    "11,BURGER KING CENTRO,,722516,Restaurantes de autoservicio,6 a 10 personas,09,"
    "Ciudad de México,015,Cuauhtémoc,19.43,-99.14,2014-12\n"
    "12,HOTEL CAMINO REAL,,721111,Hoteles con otros servicios integrados,51 a 100 personas,09,"
    "Ciudad de México,015,Cuauhtémoc,19.42,-99.17,2010-07\n"
    "12,HOTEL CAMINO REAL,,721111,Hoteles con otros servicios integrados,51 a 100 personas,09,"
    "Ciudad de México,015,Cuauhtémoc,19.42,-99.17,2010-07\n"
    "13,SIN MUNICIPIO,,722511,Restaurantes a la carta,0 a 5 personas,09,"
    "Ciudad de México,,Cuauhtémoc,19.43,-99.14,2010-07\n"
)


def zip_denue(ruta, nombre_csv, texto, codificacion):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ruta, "w") as archivo:
        archivo.writestr(nombre_csv, texto.encode(codificacion))
        archivo.writestr("diccionario_de_datos/diccionario_datos_denue.csv",
                         "campo,descripcion\nid,identificador\n")


@pytest.fixture
def raw_denue(tmp_path):
    raw = tmp_path / "raw" / "denue"
    zip_denue(raw / "2015_02" / "denue_72_25022015_csv.zip",
              "DENUE_INEGI_72_.csv", DENUE_2015, "utf-8")
    zip_denue(raw / "2019_11" / "denue_00_72_1_1119_csv.zip",
              "conjunto_de_datos/denue_inegi_72_1.csv", DENUE_2019, "latin-1")
    return raw


@pytest.fixture
def denue_processed(spark, raw_denue, tmp_path):
    interim = denue.construir_interim(spark, raw_denue, tmp_path / "csv",
                                      CONFIG["denue"]["versiones"])
    return denue.construir_processed(interim, CONFIG["denue"])


# --- DENUE ----------------------------------------------------------------------

def test_denue_une_los_dos_formatos(spark, raw_denue, tmp_path):
    interim = denue.construir_interim(spark, raw_denue, tmp_path / "csv",
                                      CONFIG["denue"]["versiones"])
    assert set(denue.COLUMNAS) <= set(interim.columns)
    filas = {(r["version"], r["id"]): r for r in interim.collect()}
    # 2015 (UTF-8, encabezados largos) y 2019 (Latin-1) con acentos correctos.
    assert filas[("2015_02", "2")]["nom_estab"] == "LA FONDA DE DOÑA MARÍA"
    assert filas[("2019_11", "10")]["municipio"] == "Naucalpan de Juárez"
    # El diccionario de datos no se cuela como datos.
    assert {r["archivo"] for r in interim.collect()} == {
        "denue_72_25022015_csv.csv", "denue_00_72_1_1119_csv.csv"}


def test_denue_processed_llaves_categorias_y_cadenas(denue_processed):
    df, metricas = denue_processed
    filas = {(r["version"], r["id"]): r for r in df.collect()}

    assert filas[("2015_02", "1")]["cve_mun5"] == "15057"
    assert filas[("2019_11", "11")]["cve_mun5"] == "09015"
    assert filas[("2015_02", "1")]["cadena"] == "McDonald's"
    assert filas[("2019_11", "10")]["cadena"] == "McDonald's"
    assert filas[("2019_11", "11")]["cadena"] == "Burger King"
    assert filas[("2019_11", "12")]["cadena"] is None
    assert filas[("2015_02", "2")]["categoria"] == "a_la_carta"
    assert filas[("2019_11", "10")]["categoria"] == "comida_rapida"
    assert filas[("2019_11", "11")]["categoria"] == "autoservicio"
    assert filas[("2019_11", "12")]["categoria"] == "hoteles"
    assert filas[("2019_11", "10")]["personal_min"] == 11
    assert filas[("2019_11", "10")]["fecha_corte"] == date(2019, 11, 1)

    assert metricas["filas_sin_llave"] == 1      # id 13 sin municipio
    assert metricas["filas_duplicadas"] == 1     # hotel repetido
    assert metricas["filas_processed"] == 5


def test_denue_sin_zip_explica_que_correr(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="download_data"):
        denue.construir_interim(spark, tmp_path, tmp_path / "csv", CONFIG["denue"]["versiones"])


class RespuestaFalsa:
    def __init__(self, contenido):
        self.contenido = contenido

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, tamano):
        yield self.contenido


def test_denue_rechaza_404_suave(monkeypatch, tmp_path):
    html = b"<!DOCTYPE html><html><body>Pagina no encontrada</body></html>"
    monkeypatch.setattr(denue_extractor.requests, "get", lambda *a, **k: RespuestaFalsa(html))
    destino = tmp_path / "x.zip"
    with pytest.raises(ValueError, match="no es un zip"):
        denue_extractor.descargar_zip("https://inegi/x.zip", destino)
    assert not destino.exists() and not list(tmp_path.glob("*.parcial"))


def test_denue_acepta_zip(monkeypatch, tmp_path):
    monkeypatch.setattr(denue_extractor.requests, "get",
                        lambda *a, **k: RespuestaFalsa(b"PK\x03\x04resto"))
    tamano, huella = denue_extractor.descargar_zip("https://inegi/x.zip", tmp_path / "x.zip")
    assert tamano == 9 and len(huella) == 64


# --- CONAPO ---------------------------------------------------------------------

MUNICIPIOS = (
    "clave_metropoli,tipo,nombre,clave_compuesta_municipio,municipio,entidad_etq,"
    "poblacion_2020,tasa_crecimiento_medio_anual_2010_2020\n"
    "09.01,Zona metropolitana,Valle de México,9015,Cuauhtémoc,Ciudad de México,545884,0.4\n"
    "09.01,Zona metropolitana,Valle de México,15057,Naucalpan de Juárez,México,834434,0.1\n"
)
CARACTERISTICAS = (
    "clave_metropoli,tipo,nombre,poblacion,tasa_crecimiento_anual_2010_2020_prct,"
    "producto_interno_bruto,poblacion_ocupada\n"
    "09.01,Zona metropolitana,Valle de México,21804515,0.9,5000000,9000000\n"
)


@pytest.fixture
def conapo(spark, tmp_path):
    (tmp_path / "municipios.csv").write_text(MUNICIPIOS, encoding="utf-8")
    (tmp_path / "caracteristicas.csv").write_text(CARACTERISTICAS, encoding="utf-8")
    return (metropolis.construir_municipios(spark, tmp_path / "municipios.csv"),
            metropolis.construir_metropolis(spark, tmp_path / "caracteristicas.csv"))


def test_metropolis_llave_a_cinco_digitos(conapo):
    municipios, metros = conapo
    assert {r["cve_mun5"] for r in municipios.collect()} == {"09015", "15057"}
    fila = metros.first()
    assert fila["metropoli"] == "Valle de México"
    assert fila["pib_per_capita"] == pytest.approx(5000000 / 21804515)


# --- Comtrade -------------------------------------------------------------------

def mes_comtrade(periodo, paises):
    datos = [{"period": periodo, "partnerCode": 0, "partnerDesc": "World",
              "customsCode": "C00", "motCode": 0, "partner2Code": 0,
              "primaryValue": sum(v for _, v, _ in paises), "netWgt": sum(k for _, _, k in paises)}]
    for codigo, valor, kg in paises:
        datos.append({"period": periodo, "partnerCode": codigo, "partnerDesc": "x",
                      "customsCode": "C00", "motCode": 0, "partner2Code": 0,
                      "primaryValue": valor, "netWgt": kg})
    # Un desglose por transporte que no debemos sumar.
    datos.append({"period": periodo, "partnerCode": 842, "motCode": 2100,
                  "customsCode": "C00", "partner2Code": 0, "primaryValue": 1e9, "netWgt": 1})
    return {"data": datos, "error": ""}


@pytest.fixture
def raw_comtrade(tmp_path):
    carpeta = tmp_path / "comtrade"
    carpeta.mkdir()
    for periodo in ("202401", "202402"):
        (carpeta / f"{periodo}.json").write_text(json.dumps(
            mes_comtrade(periodo, [(842, 16_300.0, 10_000.0), (56, 1_350.0, 1_000.0)])))
    return carpeta


def test_comtrade_periodos_cruza_anio():
    assert comtrade_extractor.periodos("2024-11", "2025-02") == [
        "202411", "202412", "202501", "202502"]


def test_comtrade_ignora_desgloses_y_calcula_precio(spark, raw_comtrade):
    df = comtrade.construir_processed(spark, raw_comtrade)
    assert df.count() == 6                          # 2 meses x (mundo + 2 paises)
    filas = {(r["periodo"], r["pais_codigo"]): r for r in df.collect()}
    assert filas[("202401", 842)]["pais"] == "Estados Unidos"
    assert filas[("202401", 842)]["usd_kg"] == pytest.approx(1.63)
    assert filas[("202401", 0)]["es_total"]


def test_comtrade_resumen_anual(spark, raw_comtrade):
    anual = comtrade.resumir_anual(comtrade.construir_processed(spark, raw_comtrade)).collect()
    assert len(anual) == 2                          # el total "Mundo" no entra
    assert sum(r["participacion"] for r in anual) == pytest.approx(100)
    belgica = next(r for r in anual if r["pais"] == "Belgica")
    assert belgica["meses"] == 2 and belgica["usd_kg"] == pytest.approx(1.35)


# --- Indice ---------------------------------------------------------------------

def serie_sintetica(spark):
    """Cuatro metropolis, dos versiones. A crece mucho, D se encoge."""
    filas = []
    datos = {  # clave: (objetivo ini, objetivo fin, cadenas ini, cadenas fin, hoteles)
        "A": (100, 200, 5, 25, 10), "B": (100, 120, 5, 8, 50),
        "C": (1000, 1100, 50, 60, 20), "D": (100, 80, 5, 4, 1),
    }
    for clave, (oi, of, ci, cf, h) in datos.items():
        for version, fecha, obj, cad in (("2019_11", date(2019, 11, 1), oi, ci),
                                         ("2026_05", date(2026, 5, 20), of, cf)):
            filas.append(Row(clave_metropoli=clave, metropoli=f"Metro {clave}", version=version,
                             fecha_corte=fecha, objetivo=obj, comida_rapida=obj // 2,
                             autoservicio=0, a_la_carta=obj // 2, cadenas=cad, hoteles=h,
                             sector_72=obj + h))
    serie = spark.createDataFrame(filas)
    metros = spark.createDataFrame([
        Row(clave_metropoli=c, tipo_metropoli="Zona metropolitana", poblacion_2020=100_000,
            pib_per_capita=1.0) for c in datos])
    return serie, metros


def test_indice_ordena_y_queda_en_rango(spark):
    serie, metros = serie_sintetica(spark)
    pesos = CONFIG["indice"]["pesos"]
    indice = ii.calcular_indice(serie, metros, "2019_11", "2026_05", pesos).toPandas()

    assert len(indice) == 4
    assert indice["indice"].between(0, 100).all()
    assert list(indice.sort_values("ranking")["ranking"]) == [1, 2, 3, 4]
    assert indice.sort_values("ranking").iloc[-1]["clave_metropoli"] == "D"

    a = indice.set_index("clave_metropoli").loc["A"]
    anios = (date(2026, 5, 20) - date(2019, 11, 1)).days / 365.25
    assert a["crecimiento_anual"] == pytest.approx(100 * (2 ** (1 / anios) - 1), rel=0.01)
    assert a["cadenas_netas_100k"] == pytest.approx(20)
    assert a["puntaje_crecimiento"] == 100
    assert ii.explicar(a).startswith("crecimiento de restaurantes")


def test_serie_por_metropoli_solo_cuenta_municipios_metropolitanos(denue_processed, conapo):
    df, _ = denue_processed
    municipios, _ = conapo
    serie = {r["version"]: r for r in ii.serie_por_metropoli(
        df, municipios, CONFIG["denue"]["categorias_objetivo"]).collect()}
    assert serie["2019_11"]["objetivo"] == 1         # McDonald's; Burger King es autoservicio
    assert serie["2019_11"]["autoservicio"] == 1
    assert serie["2019_11"]["cadenas"] == 2
    assert serie["2019_11"]["hoteles"] == 1


# --- Reglas ---------------------------------------------------------------------

def estados(resultados):
    return {(r["tabla"], r["regla"]): r["estado"] for r in resultados}


def test_reglas_denue(denue_processed):
    df, _ = denue_processed
    res = estados(ri.reglas_denue(df, CONFIG))
    assert res[("denue", "duplicados en la llave")] == "OK"
    assert res[("denue", "formato de cve_mun5")] == "OK"
    assert res[("denue", "una fecha de corte por version")] == "OK"
    # Faltan 2022_11 y 2026_05 y el volumen es diminuto: debe fallar.
    assert res[("denue", "versiones esperadas")] == "FALLA"
    assert res[("denue", "volumen por version")] == "FALLA"


def test_reglas_metropolis_e_importaciones(spark, conapo, raw_comtrade):
    municipios, metros = conapo
    res = estados(ri.reglas_metropolis(municipios, metros))
    assert res[("metropolis", "92 metropolis de CONAPO")] == "AVISO"
    assert res[("metropolis", "municipios con metropoli en el catalogo")] == "OK"

    res = estados(ri.reglas_importaciones(comtrade.construir_processed(spark, raw_comtrade),
                                          CONFIG))
    assert res[("importaciones", "total = suma de paises")] == "OK"
    assert res[("importaciones", "meses esperados")] == "AVISO"
    assert res[("importaciones", "rango de usd_kg")] == "OK"


def test_reglas_indice(spark):
    serie, metros = serie_sintetica(spark)
    indice = ii.calcular_indice(serie, metros, "2019_11", "2026_05", CONFIG["indice"]["pesos"])
    res = estados(ri.reglas_indice(indice, metros, CONFIG))
    assert set(res.values()) == {"OK"}
