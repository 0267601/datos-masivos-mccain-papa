"""
Pruebas del modelo por segmento y de la rentabilidad (escenario simulado).

Panel chico a mano: tres metropolis, seis municipios, cuatro versiones del
DENUE y cuatro segmentos con crecimientos conocidos.
"""

import json

import numpy as np
import pandas as pd
import pytest
from openpyxl import load_workbook

from src.analysis import mapa_potencial
from src.analysis import modelo_potencial as mp
from src.analysis import modelo_segmentos as ms
from src.analysis import rentabilidad as rt
from src.utils.config import cargar_config

CONFIG = cargar_config()
CFG = {**CONFIG["modelo"], "xgboost": {**CONFIG["modelo"]["xgboost"], "n_estimators": 30}}
CFG_SEG = {**CONFIG["modelo_segmentos"], "min_ejemplos_por_segmento": 5}
VERSIONES = ["2015_02", "2019_11", "2022_11", "2026_05"]
FECHAS = {"2015_02": "2015-02-01", "2019_11": "2019-11-01", "2022_11": "2022-11-01",
          "2026_05": "2026-05-01"}
MUNICIPIOS = {"01001": ("A", 0.05), "01002": ("A", 0.03), "02001": ("B", 0.01),
              "02002": ("B", 0.0), "03001": ("C", 0.08), "03002": ("C", 0.06)}


@pytest.fixture
def panel():
    filas = []
    for cve, (metro, g) in MUNICIPIOS.items():
        for version in VERSIONES:
            anios = (pd.Timestamp(FECHAS[version]) - pd.Timestamp("2015-02-01")).days / 365.25
            crece = (1 + g) ** anios
            cadenas = round(12 * (1 + 2 * g) ** anios)
            cr, alc = round(60 * crece), round(50 * crece)
            filas.append({"cve_mun5": cve, "clave_metropoli": metro, "version": version,
                          "fecha_corte": FECHAS[version], "objetivo": cadenas + cr + alc,
                          "comida_rapida": cr + cadenas // 2, "cadenas": cadenas,
                          "comida_rapida_ind": cr, "a_la_carta_ind": alc,
                          "hoteles": round(10 * (1 + g / 2) ** anios), "bares": 8,
                          "personal_promedio": 4.0, "sector_72": cadenas + cr + alc + 30})
    return pd.DataFrame(filas)


@pytest.fixture
def catalogos():
    municipios = pd.DataFrame({
        "cve_mun5": list(MUNICIPIOS),
        "clave_metropoli": [m for m, _ in MUNICIPIOS.values()],
        "poblacion_2020": [200_000, 100_000, 300_000, 50_000, 80_000, 60_000],
        "tasa_pob_2010_2020": [2.0, 1.5, 0.5, 0.2, 3.0, 2.5],
    })
    metropolis = pd.DataFrame({"clave_metropoli": ["A", "B", "C"],
                               "pib_per_capita": [0.2, 0.3, 0.15]})
    return municipios, metropolis


@pytest.fixture
def datos(panel, catalogos):
    municipios, metropolis = catalogos
    ejemplos = mp.construir_ejemplos(panel, municipios, metropolis, VERSIONES, CFG)
    return ms.ejemplos_segmentos(ejemplos, panel, municipios, VERSIONES, CFG, CFG_SEG)


@pytest.fixture
def escenario(datos):
    proyeccion, _ = ms.proyectar(datos, "compartido", CFG, CFG_SEG)
    proyeccion["metropoli"] = proyeccion["clave_metropoli"].map({"A": "Alfa", "B": "Beta",
                                                                 "C": "Gama"})
    mercado = {"usd_kg": 1.30, "kg_importados": 1_000_000.0, "anio_importaciones": 2025}
    nacional = {"cadenas": 100, "comida_rapida_ind": 1000, "a_la_carta_ind": 900, "hoteles": 200}
    matriz, generales = rt.matriz_supuestos(CONFIG["rentabilidad"], mercado, nacional)
    detalle = rt.calcular(proyeccion, matriz, generales)
    potencial = pd.DataFrame({"clave_metropoli": ["A", "B", "C"],
                              "metropoli": ["Alfa", "Beta", "Gama"],
                              "ranking": [2, 3, 1], "ranking_potencial": [1, 3, 2]})
    resumen = rt.resumir(detalle, potencial)
    return proyeccion, matriz, generales, detalle, resumen


def test_un_ejemplo_por_municipio_periodo_y_segmento(datos):
    assert len(datos) == 6 * 4 * len(ms.SEGMENTOS)
    indicadores = datos[list(ms.INDICADORES)]
    assert (indicadores.sum(axis=1) == 1).all()
    fila = datos[(datos["cve_mun5"] == "01001") & (datos["segmento"] == "comida_rapida_ind")
                 & (datos["version_base"] == "2019_11")].iloc[0]
    assert fila["crecimiento"] == pytest.approx(5.0, abs=0.3)
    assert fila["seg_crec_previo"] == pytest.approx(5.0, abs=0.3)


def test_segmentos_chicos_no_son_etiqueta(panel, catalogos, datos):
    # Los hoteles empiezan en 10 y el minimo es 5: si son etiqueta. Con un
    # minimo de 50 dejan de serlo, pero se siguen proyectando.
    assert datos.loc[datos["segmento"] == "hoteles", "crecimiento"].notna().any()
    municipios, metropolis = catalogos
    cfg = {**CFG_SEG, "min_establecimientos": {**CFG_SEG["min_establecimientos"], "hoteles": 50}}
    ejemplos = mp.construir_ejemplos(panel, municipios, metropolis, VERSIONES, CFG)
    otro = ms.ejemplos_segmentos(ejemplos, panel, municipios, VERSIONES, CFG, cfg)
    hoteles = otro[otro["segmento"] == "hoteles"]
    assert hoteles["crecimiento"].isna().all()
    assert (hoteles["version_objetivo"] == "proyeccion").sum() == 6


def test_compartido_contra_por_segmento_y_referencias(datos):
    tabla, ganador = ms.evaluar(datos, CFG, CFG_SEG, "2026_05")
    assert ganador in {"compartido", "separado"}
    assert set(tabla["segmento"]) == {"Todos", *ms.SEGMENTOS}
    assert set(tabla["modelo"]) == {"XGBoost compartido", "XGBoost por segmento",
                                    "Persistencia", "Promedio"}
    assert tabla["mae"].notna().all()


def test_proyeccion_por_metropoli_y_segmento(datos, panel):
    proyeccion, _ = ms.proyectar(datos, "separado", CFG, CFG_SEG)
    assert len(proyeccion) == 3 * len(ms.SEGMENTOS)
    hoy = panel[panel["version"] == "2026_05"][list(ms.SEGMENTOS)].sum().sum()
    assert proyeccion["locales_hoy"].sum() == pytest.approx(hoy)
    assert (proyeccion["anio_proyeccion"] == 2030).all()


def test_calibracion_con_comtrade(escenario):
    _, matriz, generales, _, _ = escenario
    total = (matriz["locales_pais"] * matriz["kg_por_local"]).sum()
    assert total == pytest.approx(generales["kg_importados"])
    fila = matriz.set_index("segmento").loc["cadenas"]
    assert fila["precio_neto"] == pytest.approx(2.20 * 0.75)
    assert fila["margen_usd_kg"] == pytest.approx(2.20 * 0.75 - 1.30 - 0.20 - 0.05)
    # Las cadenas ganan menos por kilo que los independientes.
    margen = matriz.set_index("segmento")["margen_pct"]
    assert margen["cadenas"] < margen["hoteles"] < margen["comida_rapida_ind"]


def test_utilidad_y_resumen(escenario):
    proyeccion, matriz, generales, detalle, resumen = escenario
    fila = detalle.iloc[0]
    esperado = (fila["locales_proyectados"] * fila["kg_por_local"]
                * generales["participacion_mccain"] * fila["margen_usd_kg"])
    assert fila["utilidad_proy_usd"] == pytest.approx(esperado)
    assert resumen["utilidad_proy_usd"].sum() == pytest.approx(detalle["utilidad_proy_usd"].sum())
    assert list(resumen["ranking_utilidad"]) == [1, 2, 3]
    assert rt.validar(detalle, resumen, proyeccion)["estado"] == "VALIDADO"


def test_excel_con_formulas(escenario, tmp_path):
    _, matriz, generales, detalle, resumen = escenario
    ruta = rt.escribir_excel(detalle, resumen, matriz, generales, tmp_path / "r.xlsx")
    libro = load_workbook(ruta)
    assert libro.sheetnames == ["Leeme", "Supuestos", "Calculo", "Resumen", "Sensibilidad"]
    calc = libro["Calculo"]
    assert calc["Q2"].value == "=K2*M2"                         # utilidad 2030
    assert calc["H2"].value.startswith("=INDEX(Supuestos!")
    assert libro["Supuestos"]["B5"].value == pytest.approx(2.20)
    assert libro["Supuestos"]["B10"].value.startswith("=IF(SUMPRODUCT(")
    assert calc.max_row >= len(detalle) + 1


def test_html_con_utilidad(escenario, tmp_path):
    _, matriz, generales, detalle, resumen = escenario
    tabla = pd.DataFrame({
        "clave_metropoli": ["A", "B", "C"], "metropoli": ["Alfa", "Beta", "Gama"],
        "tipo_metropoli": "Zona metropolitana", "latitud": [20.0, 21.0, 22.0],
        "longitud": [-100.0, -101.0, -102.0], "ranking_potencial": [1, 2, 3],
        "ranking": [2, 3, 1], "indice": [60.0, 50.0, 70.0], "crecimiento_anual": [3.0, 1.0, 5.0],
        "crecimiento_esperado": [2.5, 1.0, 4.0], "restaurantes_hoy": [300, 200, 100],
        "restaurantes_proyectados": [330, 205, 120], "potencial_hoy_usd": [1e6, 7e5, 3e5],
        "potencial_proyectado_usd": [1.1e6, 7.2e5, 3.6e5],
        "potencial_incremental_usd": [1e5, 2e4, 6e4], "cadenas_netas_100k": [1.0, 0.5, 2.0],
        "hoteles_100k": [5.0, 3.0, 9.0], "poblacion_2020": [3e5, 3.5e5, 1.4e5],
        "anio_proyeccion": 2030, "impulsores": "[]"})
    metricas = pd.DataFrame({"modelo": ["XGBoost", "Persistencia", "Promedio"],
                             "mae": [1.0, 1.5, 2.0], "spearman_metropolis": [0.6, 0.4, np.nan],
                             "periodo_prueba": "2022_11 -> 2026_05"})
    metricas_seg = pd.DataFrame({"segmento": "Todos", "modelo": ["XGBoost compartido",
                                                                 "XGBoost por segmento"],
                                 "mae": [1.0, 1.2]})
    html = mapa_potencial.generar(
        tabla, metricas, {"kg_por_restaurante": 100.0, "usd_kg": 1.3, "anio_importaciones": 2025},
        pd.Series({"Hoteles": 1.0}), tmp_path / "m.html",
        rentabilidad={"resumen": resumen, "detalle": detalle, "matriz": matriz,
                      "generales": generales, "metricas": metricas_seg,
                      "ganador": "compartido", "cfg_inversion": CONFIG["inversion"]})
    datos = json.loads(html.split("const D = ")[1].split(";\nconst C")[0].replace("<\\/", "</"))
    r = datos["contexto"]["rentabilidad"]
    assert r["ganador"] == "compartido"
    # La pagina recibe los insumos crudos y los supuestos editables.
    assert len(r["supuestos"]["segmentos"]) == len(ms.SEGMENTOS)
    assert r["supuestos"]["presupuesto"] == CONFIG["inversion"]["presupuesto_usd"]
    metro = datos["metros"][0]
    assert set(metro["seg"]) == set(ms.SEGMENTOS)
    assert 'id="vistas"' in html and 'id="supuestos"' in html


# ---------------------------------------------------------------------------
# Si tuviera X millones
# ---------------------------------------------------------------------------

from src.analysis import inversion as inv  # noqa: E402

CFG_INV = CONFIG["inversion"]


def test_capacidad_sin_cadenas(escenario):
    *_, detalle, _ = escenario
    cap = inv.capacidades(detalle, CFG_INV)
    assert len(cap) == 3
    fila = cap.set_index("clave_metropoli").loc["C"]
    propios = detalle[(detalle["clave_metropoli"] == "C")
                      & detalle["segmento"].isin(CFG_INV["segmentos_capturables"])]
    assert fila["kg_proy"] == pytest.approx(propios["kg_mercado_proy"].sum())
    assert fila["mitad_usd"] == pytest.approx(CFG_INV["costo_mitad_usd_por_kg"] * fila["kg_proy"])


def test_reparto_suma_x_e_iguala_el_ultimo_dolar(escenario):
    *_, detalle, _ = escenario
    cap = inv.capacidades(detalle, CFG_INV)
    x = 0.5 * cap["mitad_usd"].sum()
    reparto = inv.asignar(cap, x)
    assert reparto["inversion_usd"].sum() == pytest.approx(x)
    assert inv.validar(reparto, x)["estado"] == "VALIDADO"
    # En las ciudades con inversion, el ultimo dolar rinde lo mismo.
    con = reparto[reparto["inversion_usd"] > 1]
    marginal = con["tope_usd"] / con["escala_usd"] * np.exp(-con["inversion_usd"] / con["escala_usd"])
    assert marginal.max() == pytest.approx(marginal.min(), rel=1e-3)


def test_rendimientos_decrecientes(escenario):
    *_, detalle, _ = escenario
    cap = inv.capacidades(detalle, CFG_INV)
    base = cap["mitad_usd"].sum()
    tabla = inv.curva(cap, [base * f for f in (0.25, 0.5, 1, 2, 4)])
    assert tabla["contribucion_extra_usd"].is_monotonic_increasing
    assert tabla["retorno_del_tramo"].is_monotonic_decreasing
    assert tabla["anios_recuperacion"].is_monotonic_increasing
    assert (tabla["contribucion_extra_usd"] <= cap["tope_usd"].sum() + 1e-6).all()


def test_excel_con_hoja_de_inversion(escenario, tmp_path):
    _, matriz, generales, detalle, resumen = escenario
    reparto = inv.asignar(inv.capacidades(detalle, CFG_INV), CFG_INV["presupuesto_usd"])
    ruta = rt.escribir_excel(detalle, resumen, matriz, generales, tmp_path / "r.xlsx",
                             inversion={"cfg": CFG_INV, "reparto": reparto})
    libro = load_workbook(ruta)
    assert libro.sheetnames[1] == "Inversion" and "Rejilla" in libro.sheetnames
    hoja = libro["Inversion"]
    assert hoja["B4"].value == CFG_INV["presupuesto_usd"]
    assert hoja["K31"].value.startswith("=IF(SUM($J$31:")
    assert libro["Rejilla"]["B4"].value.startswith("=SUMPRODUCT(")
