"""
Pruebas del modelo de potencial, del heatmap HTML, del orquestador y de la
muestra real del indice.

Armamos un panel chico a mano: tres metropolis, seis municipios y cuatro
versiones del DENUE, con crecimientos conocidos.
"""

import json
import zipfile

import numpy as np
import pandas as pd
import pytest

from src import pipeline
from src.analysis import mapa_potencial
from src.analysis import modelo_potencial as mp
from src.ingestion import muestra_indice
from src.processing import denue
from src.utils.config import cargar_config

CONFIG = cargar_config()
CFG = {**CONFIG["modelo"], "xgboost": {**CONFIG["modelo"]["xgboost"], "n_estimators": 30}}
VERSIONES = ["2015_02", "2019_11", "2022_11", "2026_05"]
FECHAS = {"2015_02": "2015-02-01", "2019_11": "2019-11-01", "2022_11": "2022-11-01",
          "2026_05": "2026-05-01"}


@pytest.fixture
def panel():
    filas = []
    municipios = {"01001": ("A", 0.05), "01002": ("A", 0.03), "02001": ("B", 0.01),
                  "02002": ("B", 0.0), "03001": ("C", 0.08), "03002": ("C", 0.06)}
    for cve, (_, g) in municipios.items():
        for version in VERSIONES:
            anios = (pd.Timestamp(FECHAS[version]) - pd.Timestamp("2015-02-01")).days / 365.25
            objetivo = round(100 * (1 + g) ** anios)
            filas.append({"cve_mun5": cve, "clave_metropoli": municipios[cve][0],
                          "version": version, "fecha_corte": FECHAS[version],
                          "objetivo": objetivo, "comida_rapida": objetivo // 2,
                          "cadenas": objetivo // 10, "hoteles": 5, "bares": 8,
                          "personal_promedio": 4.0, "sector_72": objetivo + 20})
    return pd.DataFrame(filas)


@pytest.fixture
def catalogos():
    municipios = pd.DataFrame({
        "cve_mun5": ["01001", "01002", "02001", "02002", "03001", "03002"],
        "clave_metropoli": ["A", "A", "B", "B", "C", "C"],
        "poblacion_2020": [200_000, 100_000, 300_000, 50_000, 80_000, 60_000],
        "tasa_pob_2010_2020": [2.0, 1.5, 0.5, 0.2, 3.0, 2.5],
    })
    metropolis = pd.DataFrame({"clave_metropoli": ["A", "B", "C"],
                               "pib_per_capita": [0.2, 0.3, 0.15]})
    return municipios, metropolis


@pytest.fixture
def ejemplos(panel, catalogos):
    municipios, metropolis = catalogos
    return mp.construir_ejemplos(panel, municipios, metropolis, VERSIONES, CFG)


def test_tasa_anual():
    assert mp.tasa_anual([121], [100], 2)[0] == pytest.approx(10.0)
    assert np.isnan(mp.tasa_anual([5], [0], 2)[0])


def test_ejemplos_una_fila_por_municipio_y_transicion(ejemplos):
    # 6 municipios x (3 transiciones con etiqueta + 1 de proyeccion)
    assert len(ejemplos) == 24
    assert set(ejemplos["version_objetivo"]) == {"2019_11", "2022_11", "2026_05", "proyeccion"}
    primera = ejemplos[ejemplos["version_base"] == "2015_02"]
    assert primera["crec_previo"].isna().all()           # no hay version anterior
    fila = ejemplos[(ejemplos["cve_mun5"] == "03001")
                    & (ejemplos["version_objetivo"] == "2022_11")].iloc[0]
    assert fila["crecimiento"] == pytest.approx(8.0, abs=0.6)
    assert fila["crec_previo"] == pytest.approx(8.0, abs=0.6)
    futuro = ejemplos[ejemplos["version_objetivo"] == "proyeccion"]
    assert futuro["crecimiento"].isna().all() and (futuro["anios"] == 4).all()


def test_municipios_chicos_no_son_etiqueta(panel, catalogos):
    municipios, metropolis = catalogos
    chico = panel.assign(objetivo=np.where(panel["cve_mun5"] == "02002", 3, panel["objetivo"]))
    ejemplos = mp.construir_ejemplos(chico, municipios, metropolis, VERSIONES, CFG)
    etiquetas = ejemplos[(ejemplos["cve_mun5"] == "02002")
                         & (ejemplos["version_objetivo"] != "proyeccion")]
    assert etiquetas["crecimiento"].isna().all()


def test_evaluacion_temporal_y_referencias(ejemplos):
    tabla, prueba = mp.evaluar_modelo(ejemplos, CFG, "2026_05")
    assert list(tabla["modelo"]) == ["XGBoost", "Persistencia", "Promedio"]
    assert (prueba["version_objetivo"] == "2026_05").all()     # nunca entrena con la prueba
    assert tabla["municipios_entrenamiento"].iloc[0] == 12
    assert (tabla["mae"] >= 0).all()
    # Crecimientos constantes por municipio: repetir el anterior es casi perfecto.
    persistencia = tabla.set_index("modelo").loc["Persistencia"]
    assert persistencia["mae"] < 0.6


def test_parametros_de_mercado_usan_el_ultimo_anio_completo():
    anual = pd.DataFrame({"anio": [2024, 2024, 2025], "pais": ["EU", "CA", "EU"],
                          "valor_usd": [150.0, 50.0, 30.0], "kg": [100.0, 50.0, 20.0],
                          "meses": [12, 12, 5]})
    mercado = mp.parametros_mercado(anual, objetivo_nacional=10)
    assert mercado["anio_importaciones"] == 2024
    assert mercado["kg_por_restaurante"] == pytest.approx(15.0)
    assert mercado["usd_kg"] == pytest.approx(200 / 150)


def potencial(ejemplos):
    modelo = mp.entrenar_xgboost(ejemplos, CFG)
    mercado = {"kg_por_restaurante": 100.0, "usd_kg": 2.0}
    indice = pd.DataFrame({"clave_metropoli": ["A", "B", "C"], "metropoli": ["Metro A", "Metro B", "Metro C"],
                           "tipo_metropoli": "Zona metropolitana", "ranking": [1, 2, 3],
                           "indice": [80.0, 50.0, 70.0], "crecimiento_anual": [4.0, 0.5, 7.0],
                           "cadenas_netas_100k": [1.0, 0.1, 2.0], "hoteles_100k": [5.0, 3.0, 9.0],
                           "poblacion_2020": [300_000, 350_000, 140_000],
                           "pib_per_capita": [0.2, 0.3, 0.15]})
    centroides = pd.DataFrame({"clave_metropoli": ["A", "B", "C"],
                               "latitud": [20.0, 25.0, 19.0], "longitud": [-100.0, -105.0, -99.0]})
    tabla, _ = mp.proyectar_potencial(modelo, ejemplos, indice, mercado, centroides)
    return tabla


def test_potencial_por_metropoli(ejemplos):
    tabla = potencial(ejemplos)
    assert len(tabla) == 3 and list(tabla["ranking_potencial"]) == [1, 2, 3]
    fila = tabla.set_index("clave_metropoli").loc["A"]
    assert fila["potencial_hoy_usd"] == pytest.approx(fila["restaurantes_hoy"] * 200)
    assert fila["potencial_incremental_usd"] == pytest.approx(
        fila["potencial_proyectado_usd"] - fila["potencial_hoy_usd"])
    assert fila["anio_proyeccion"] == 2030
    assert len(json.loads(fila["impulsores"])) == 2
    # La metropoli que mas crecio en la historia debe proyectar mas crecimiento.
    crec = tabla.set_index("clave_metropoli")["crecimiento_esperado"]
    assert crec["C"] > crec["B"]


def test_html_interactivo(ejemplos, tmp_path):
    tabla = potencial(ejemplos)
    metricas = pd.DataFrame({"modelo": ["XGBoost", "Persistencia", "Promedio"],
                             "mae": [1.0, 1.5, 2.0], "spearman_metropolis": [0.6, 0.4, np.nan],
                             "periodo_prueba": "2022_11 -> 2026_05"})
    mercado = {"kg_por_restaurante": 100.0, "usd_kg": 2.0, "anio_importaciones": 2025}
    importancia = pd.Series({"Crecimiento del periodo anterior (%)": 0.6, "Hoteles": 0.4})
    tabla.loc[0, "metropoli"] = "</script><b>X"          # no debe romper el HTML
    html = mapa_potencial.generar(tabla, metricas, mercado, importancia, tmp_path / "mapa.html")
    assert (tmp_path / "mapa.html").exists()
    assert html.count("</script>") == 1                     # solo el cierre real
    datos = json.loads(html.split("const D = ")[1].split(";\nconst C")[0].replace("<\\/", "</"))
    assert len(datos["metros"]) == 3 and datos["contexto"]["anio"] == 2030
    assert any("Puesto 1 de 3" in s for s in datos["metros"][0]["insights"])


def test_ejecutar_etapa_registra_y_detiene():
    registro = []
    assert pipeline.ejecutar_etapa("ok", lambda: 0, registro) == 0
    with pytest.raises(RuntimeError, match="codigo 1"):
        pipeline.ejecutar_etapa("falla", lambda: 1, registro)
    assert pipeline.ejecutar_etapa("tolerada", lambda: 1, registro, permitir_fallas=True) == 1
    assert [e["estado"] for e in registro] == ["OK", "ERROR", "OK_CON_FALLAS"]
    assert all("duracion_segundos" in e for e in registro)


def test_leer_configuracion():
    cfg = pipeline.leer_configuracion(["--fuente", "indice", "--sin-descarga"])
    assert cfg == {"fuente": "indice", "sin_descarga": True, "muestra": False,
                   "permitir_fallas": False}


def test_zip_de_muestra_se_lee_igual_que_el_original(spark, tmp_path):
    """El extracto que arma Databricks debe pasar por el mismo lector del DENUE."""
    fila = {c: "" for c in denue.COLUMNAS}
    fila.update(id="7", nom_estab="MCDONALD'S CENTRO", nombre_act="Restaurantes de autoservicio",
                cve_ent="22", cve_mun="014", municipio="Querétaro", latitud="20.59",
                longitud="-100.39", per_ocu="11 a 30 personas")
    df = pd.DataFrame([fila])[denue.COLUMNAS]
    for etiqueta in ("2015_02", "2026_05"):
        carpeta = tmp_path / "raw" / etiqueta
        carpeta.mkdir(parents=True)
        (carpeta / f"muestra_{etiqueta}.zip").write_bytes(muestra_indice.zip_version(df, etiqueta))
        with zipfile.ZipFile(carpeta / f"muestra_{etiqueta}.zip") as z:
            nombres = z.namelist()
        assert nombres == (["DENUE_INEGI_72_.csv"] if etiqueta == "2015_02"
                           else ["conjunto_de_datos/denue_inegi_72_.csv"])
    interim = denue.construir_interim(spark, tmp_path / "raw", tmp_path / "csv",
                                      CONFIG["denue"]["versiones"])
    filas = {r["version"]: r for r in interim.collect()}
    assert filas["2015_02"]["municipio"] == "Querétaro"         # UTF-8 y encabezados largos
    assert filas["2026_05"]["municipio"] == "Querétaro"         # Latin-1 y encabezados cortos
    assert filas["2026_05"]["nom_estab"] == "MCDONALD'S CENTRO"


def test_rechazos_con_motivo(spark):
    filas = [("1", "A", "Restaurantes de autoservicio", "09", "015", "2019_11"),
             (None, "B", "Restaurantes de autoservicio", "09", "015", "2019_11"),
             ("3", "C", "Restaurantes de autoservicio", None, None, "2019_11"),
             ("1", "A", "Restaurantes de autoservicio", "09", "015", "2019_11")]
    df = spark.createDataFrame(filas, "id string, nom_estab string, nombre_act string, "
                                      "cve_ent string, cve_mun string, version string")
    for c in denue.COLUMNAS:
        if c not in df.columns:
            df = df.withColumn(c, df["id"].cast("string").substr(0, 0))
    df = df.withColumn("archivo", df["version"])
    salida = denue.construir_silver(df, CONFIG["denue"])
    motivos = sorted(r["motivos_rechazo"] for r in salida["rechazos"].collect())
    assert motivos == ["id_invalido", "municipio_invalido"]
    assert (salida["filas"], salida["n_rechazos"], salida["n_duplicados"]) == (1, 2, 1)
