"""
Modelo XGBoost por segmento: cuanto crece cada tipo de cliente.

El modelo de potencial predice el crecimiento de todos los restaurantes
objetivo juntos. Para la rentabilidad necesitamos saber cuanto crece cada
segmento, porque a cada uno se le vende a distinto precio y margen:

    cadenas             sucursales de cadenas que venden papas (cuenta nacional)
    comida_rapida_ind   comida rapida independiente
    a_la_carta_ind      restaurantes a la carta independientes
    hoteles             hoteles

Probamos dos formas y dejamos que el periodo de prueba decida:

    compartido   un solo XGBoost con todos los segmentos; el segmento entra
                 como variable. Aprende lo comun con todos los datos.
    separado     un XGBoost por segmento. Mas facil de explicar, pero las
                 cadenas tienen pocos municipios con suficientes sucursales.

Las dos se comparan, por segmento, contra persistencia y promedio en el
mismo periodo que el modelo no vio (2022 -> 2026). El ganador se reentrena
con los tres periodos y proyecta 2026 -> 2030.

Unidad: un municipio metropolitano, en un periodo, en un segmento.
"""

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.analysis.modelo_potencial import VARIABLES, metricas, tasa_anual

SEGMENTOS = {
    "cadenas": "Cadenas (cuenta nacional)",
    "comida_rapida_ind": "Comida rápida independiente",
    "a_la_carta_ind": "A la carta independiente",
    "hoteles": "Hoteles",
}

# Variables del segmento, ademas de las del municipio (VARIABLES).
VARIABLES_SEGMENTO = {
    "log_seg": "Locales del segmento (log)",
    "seg_100k": "Locales del segmento por 100 mil hab.",
    "seg_crec_previo": "Crecimiento previo del segmento (%)",
}
INDICADORES = {f"es_{s}": f"Segmento: {n}" for s, n in SEGMENTOS.items()}

COLUMNAS_SEPARADO = list(VARIABLES) + list(VARIABLES_SEGMENTO)
COLUMNAS_COMPARTIDO = COLUMNAS_SEPARADO + list(INDICADORES)
NOMBRES = {**VARIABLES, **VARIABLES_SEGMENTO, **INDICADORES}


# ---------------------------------------------------------------------------
# 1. Ejemplos por segmento
# ---------------------------------------------------------------------------

def ejemplos_segmentos(ejemplos, panel, municipios, versiones, cfg_modelo, cfg_seg):
    """Repetimos cada ejemplo del modelo de potencial una vez por segmento.

    ejemplos: salida de construir_ejemplos (variables del municipio al inicio
    de cada periodo). Aqui agregamos los conteos del segmento al inicio, al
    final y en la version anterior, y la etiqueta: su crecimiento anual.
    """
    panel = panel.copy()
    panel["fecha_corte"] = pd.to_datetime(panel["fecha_corte"])
    conteos = panel.pivot_table(index="cve_mun5", columns="version",
                                values=list(SEGMENTOS), aggfunc="first")
    fechas = panel.groupby("version")["fecha_corte"].first()
    anterior = {v: versiones[i - 1] for i, v in enumerate(versiones) if i > 0}
    poblacion = municipios.set_index("cve_mun5")["poblacion_2020"]

    def contar(segmento, version_por_fila, claves):
        salida = np.full(len(claves), np.nan)
        for version in pd.unique(version_por_fila.dropna()):
            if (segmento, version) not in conteos.columns:
                continue
            filas = (version_por_fila == version).to_numpy()
            salida[filas] = conteos[(segmento, version)].reindex(claves[filas]).to_numpy()
        return salida

    claves = ejemplos["cve_mun5"].to_numpy()
    con_etiqueta = ejemplos["version_objetivo"] != "proyeccion"
    previa = ejemplos["version_base"].map(anterior)
    anios_previos = ((ejemplos["version_base"].map(fechas) - previa.map(fechas))
                     .dt.days / 365.25)
    por_100k = 100_000 / ejemplos["cve_mun5"].map(poblacion).where(lambda p: p > 0)

    bloques = []
    for segmento in SEGMENTOS:
        d = ejemplos.copy()
        d["segmento"] = segmento
        base = np.nan_to_num(contar(segmento, ejemplos["version_base"], claves))
        final = contar(segmento, ejemplos["version_objetivo"], claves)
        final = np.where(con_etiqueta, np.nan_to_num(final), np.nan)
        d["seg_base"] = base
        d["log_seg"] = np.log1p(base)
        d["seg_100k"] = base * por_100k
        d["seg_crec_previo"] = tasa_anual(base, contar(segmento, previa, claves),
                                          anios_previos.to_numpy())
        for otro in SEGMENTOS:
            d[f"es_{otro}"] = int(otro == segmento)

        crec = tasa_anual(final, base, d["anios"].to_numpy())
        # Con muy pocos locales la tasa es ruido: no la usamos como etiqueta.
        crec = np.where(base >= cfg_seg["min_establecimientos"][segmento], crec, np.nan)
        d["crecimiento"] = np.clip(crec, *cfg_modelo["limites_crecimiento"])
        bloques.append(d)

    datos = pd.concat(bloques, ignore_index=True)
    datos[COLUMNAS_COMPARTIDO] = datos[COLUMNAS_COMPARTIDO].replace([np.inf, -np.inf], np.nan)
    return datos


# ---------------------------------------------------------------------------
# 2. Entrenar: compartido o separado
# ---------------------------------------------------------------------------

def _xgboost(datos, columnas, cfg_modelo):
    from xgboost import XGBRegressor

    modelo = XGBRegressor(**cfg_modelo["xgboost"])
    modelo.fit(datos[columnas], datos["crecimiento"], sample_weight=np.sqrt(datos["seg_base"]))
    return modelo


def entrenar(datos, forma, cfg_modelo, cfg_seg):
    """Devolvemos una funcion predecir(df) para la forma pedida."""
    datos = datos.dropna(subset=["crecimiento"])
    compartido = _xgboost(datos, COLUMNAS_COMPARTIDO, cfg_modelo)
    if forma == "compartido":
        modelos = {"compartido": compartido}
    else:
        modelos = {}
        for segmento in SEGMENTOS:
            propios = datos[datos["segmento"] == segmento]
            # Un segmento con muy pocos ejemplos se queda con el compartido.
            if len(propios) >= cfg_seg["min_ejemplos_por_segmento"]:
                modelos[segmento] = _xgboost(propios, COLUMNAS_SEPARADO, cfg_modelo)

    def predecir(df):
        salida = pd.Series(compartido.predict(df[COLUMNAS_COMPARTIDO]), index=df.index)
        for segmento, modelo in modelos.items():
            if segmento == "compartido":
                continue
            filas = df["segmento"] == segmento
            if filas.any():
                salida[filas] = modelo.predict(df.loc[filas, COLUMNAS_SEPARADO])
        return salida.clip(*cfg_modelo["limites_crecimiento"])

    predecir.modelos = modelos
    predecir.compartido = compartido
    return predecir


def evaluar(datos, cfg_modelo, cfg_seg, version_prueba):
    """Compartido vs separado vs referencias, por segmento y en total."""
    con_etiqueta = datos.dropna(subset=["crecimiento"])
    entrenamiento = con_etiqueta[con_etiqueta["version_objetivo"] != version_prueba]
    prueba = con_etiqueta[con_etiqueta["version_objetivo"] == version_prueba].copy()
    if entrenamiento.empty or prueba.empty:
        raise ValueError("Se necesitan al menos tres versiones del DENUE para validar")

    prueba["pred_compartido"] = entrenar(entrenamiento, "compartido", cfg_modelo, cfg_seg)(prueba)
    prueba["pred_separado"] = entrenar(entrenamiento, "separado", cfg_modelo, cfg_seg)(prueba)
    promedios = {s: np.average(g["crecimiento"], weights=np.sqrt(g["seg_base"]))
                 for s, g in entrenamiento.groupby("segmento")}
    general = np.average(entrenamiento["crecimiento"], weights=np.sqrt(entrenamiento["seg_base"]))
    prueba["pred_promedio"] = prueba["segmento"].map(promedios).fillna(general)
    prueba["pred_persistencia"] = prueba["seg_crec_previo"].fillna(prueba["pred_promedio"])

    filas = []
    grupos = [("Todos", prueba)] + [(s, prueba[prueba["segmento"] == s]) for s in SEGMENTOS]
    for segmento, grupo in grupos:
        if grupo.empty:
            continue
        for nombre, columna in [("XGBoost compartido", "pred_compartido"),
                                ("XGBoost por segmento", "pred_separado"),
                                ("Persistencia", "pred_persistencia"),
                                ("Promedio", "pred_promedio")]:
            filas.append({"segmento": segmento, "modelo": nombre,
                          **metricas(grupo["crecimiento"], grupo[columna],
                                     np.sqrt(grupo["seg_base"])),
                          "ejemplos_prueba": len(grupo),
                          "ejemplos_entrenamiento": int(
                              len(entrenamiento) if segmento == "Todos"
                              else (entrenamiento["segmento"] == segmento).sum())})
    tabla = pd.DataFrame(filas)
    tabla["periodo_prueba"] = f"{prueba['version_base'].iloc[0]} -> {version_prueba}"

    total = tabla[tabla["segmento"] == "Todos"].set_index("modelo")["mae"]
    ganador = ("compartido" if total["XGBoost compartido"] <= total["XGBoost por segmento"]
               else "separado")
    tabla["ganador"] = ganador
    return tabla, ganador


# ---------------------------------------------------------------------------
# 3. Proyectar por metropoli y segmento
# ---------------------------------------------------------------------------

def proyectar(datos, forma, cfg_modelo, cfg_seg):
    """Locales hoy y en el anio de proyeccion, por metropoli y segmento."""
    predecir = entrenar(datos, forma, cfg_modelo, cfg_seg)
    futuro = datos[datos["version_objetivo"] == "proyeccion"].copy()
    futuro["crecimiento_esperado"] = predecir(futuro)
    futuro["locales_proyectados"] = (futuro["seg_base"]
                                     * (1 + futuro["crecimiento_esperado"] / 100) ** futuro["anios"])
    tabla = (futuro.groupby(["clave_metropoli", "segmento"])
             .agg(locales_hoy=("seg_base", "sum"),
                  locales_proyectados=("locales_proyectados", "sum"),
                  anios=("anios", "first"), fecha_base=("fecha_base", "first"))
             .reset_index())
    tabla["crecimiento_esperado"] = tasa_anual(tabla["locales_proyectados"],
                                               tabla["locales_hoy"], tabla["anios"])
    tabla["anio_proyeccion"] = (pd.to_datetime(tabla["fecha_base"]).dt.year
                                + tabla["anios"].round().astype(int))
    return tabla.drop(columns=["fecha_base"]), predecir


def importancia(predecir):
    """Ganancia de cada variable en el modelo compartido (el que siempre existe)."""
    ganancia = predecir.compartido.get_booster().get_score(importance_type="gain")
    total = sum(ganancia.values()) or 1
    return (pd.Series({NOMBRES[k]: v / total for k, v in ganancia.items()})
            .sort_values(ascending=False))


def conteo_nacional(denue, version):
    """Locales de cada segmento en todo el pais (para calibrar el consumo)."""
    sin_cadena = F.col("cadena").isNull()
    fila = (denue.filter(F.col("version") == version)
            .agg(F.sum((F.col("categoria").isin("comida_rapida", "a_la_carta")
                        & ~sin_cadena).cast("int")).alias("cadenas"),
                 F.sum(((F.col("categoria") == "comida_rapida") & sin_cadena)
                       .cast("int")).alias("comida_rapida_ind"),
                 F.sum(((F.col("categoria") == "a_la_carta") & sin_cadena)
                       .cast("int")).alias("a_la_carta_ind"),
                 F.sum((F.col("categoria") == "hoteles").cast("int")).alias("hoteles"))
            .first())
    return {s: int(fila[s] or 0) for s in SEGMENTOS}
