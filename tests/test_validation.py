"""Pruebas de las reglas: cada una debe detectar el problema que plantamos."""

from datetime import date

import pytest

from src.validation import reglas as r


@pytest.fixture
def df_precios(spark):
    filas = [
        (date(2025, 3, 3), 2025, 3, "Alpha", 18.0, 20.0, 22.0),
        (date(2025, 3, 3), 2025, 3, "Alpha", 18.0, 20.0, 22.0),     # duplicado
        (date(2025, 3, 4), 2025, 3, "Gema", 25.0, 21.0, 23.0),      # min > frec
        (date(2030, 1, 1), 2030, 1, "Gema", 10.0, 500.0, 600.0),    # futuro y fuera de rango
        (date(2025, 4, 1), 2025, 3, "Alpha", None, None, None),     # particion mal y nulos
    ]
    return spark.createDataFrame(
        filas, "fecha date, anio int, mes int, variedad string, "
               "precio_min double, precio_frec double, precio_max double")


def test_exigir_columnas_lanza_error(df_precios):
    with pytest.raises(ValueError, match="Faltan columnas"):
        r.exigir_columnas(df_precios, ["fecha", "no_existe"])


def test_validar_columnas(df_precios):
    assert r.validar_columnas(df_precios, ["fecha"], "t")["estado"] == r.OK
    assert r.validar_columnas(df_precios, ["x"], "t")["estado"] == r.FALLA


def test_validar_tipos(df_precios):
    assert r.validar_tipos(df_precios, {"fecha": "date"}, "t")["estado"] == r.OK
    assert r.validar_tipos(df_precios, {"fecha": "string"}, "t")["estado"] == r.FALLA


def test_validar_nulos(df_precios):
    res = {x["regla"]: x for x in r.validar_nulos(df_precios, {"precio_frec": 0, "fecha": 0}, "t")}
    assert res["nulos en precio_frec"]["estado"] == r.FALLA
    assert res["nulos en precio_frec"]["valor"] == 20.0
    assert res["nulos en fecha"]["estado"] == r.OK


def test_validar_duplicados(df_precios):
    res = r.validar_duplicados(df_precios, ["fecha", "variedad", "precio_frec"], "t")
    assert res["estado"] == r.FALLA and res["valor"] == 1


def test_validar_rango_y_orden(df_precios):
    assert r.validar_rango(df_precios, "precio_frec", 1, 100, "t")["valor"] == 1
    assert r.validar_rango(df_precios, "precio_frec", 1, 100, "t", nivel=r.AVISO)["estado"] == r.AVISO
    assert r.validar_orden(df_precios, "precio_min", "precio_frec", "t")["valor"] == 1


def test_validar_fechas(df_precios):
    res = r.validar_fechas(df_precios, "t", hoy=date(2026, 10, 21))
    assert res["estado"] == r.FALLA
    assert res["valor"] == 2          # una en el futuro, una en la particion equivocada


def test_validar_anios(df_precios):
    assert r.validar_anios(df_precios, "anio", [2025, 2030], "t")["estado"] == r.OK
    res = r.validar_anios(df_precios, "anio", [2024, 2025], "t")
    assert res["estado"] == r.FALLA and "2024" in res["detalle"]


def test_validar_volumen(df_precios):
    assert r.validar_volumen_minimo(df_precios, 10, "t")["estado"] == r.FALLA
    res = r.validar_volumen_por_anio(df_precios, "anio", 2, 10, "t")
    assert res["estado"] == r.FALLA and "2030" in res["detalle"]


def test_validar_porcentajes(spark):
    bien = spark.createDataFrame([(2025, 60.0, 40.0), (2025, 40.0, 60.0)],
                                 "anio int, pct_nacional double, pct_mayoreo double")
    mal = spark.createDataFrame([(2025, 60.0, 40.0)],
                                "anio int, pct_nacional double, pct_mayoreo double")
    assert r.validar_porcentajes(bien)["estado"] == r.OK
    assert r.validar_porcentajes(mal)["estado"] == r.FALLA


def test_validar_truncamiento():
    sano = {"archivos": {"a": {"fuente": "SNIIM", "truncado": False, "estado": "ok"}}}
    truncado = {"archivos": {"a": {"fuente": "SNIIM", "truncado": True, "estado": "ok"}}}
    assert r.validar_truncamiento(sano)["estado"] == r.OK
    assert r.validar_truncamiento(truncado)["estado"] == r.FALLA
    assert r.validar_truncamiento(None)["estado"] == r.AVISO


def test_validar_truncamiento_ignora_errores_de_otras_fuentes():
    otra = {"archivos": {"a": {"fuente": "SNIIM", "truncado": False, "estado": "ok"},
                         "b": {"fuente": "CONAPO", "estado": "error"}}}
    assert r.validar_truncamiento(otra)["estado"] == r.OK
