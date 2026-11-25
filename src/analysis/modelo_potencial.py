"""
Modelo de crecimiento con XGBoost y potencial de mercado por metropoli.

Pregunta:
    Si McCain invierte hoy, ¿donde crecera mas el mercado de papas a la
    francesa en los proximos anios, y cuanto vale ese crecimiento en dolares?

Ejecucion desde la raiz del repositorio (despues de indice_inversion):

    python -m src.analysis.modelo_potencial

Como lo hacemos:

    1. Panel (Spark)       una fila por municipio metropolitano y version del
                           DENUE: restaurantes objetivo, cadenas, hoteles,
                           bares, personal ocupado.
    2. Ejemplos            para cada par de versiones consecutivas, las
                           variables al inicio y el crecimiento anual que
                           ocurrio despues (la etiqueta).
    3. Validacion temporal entrenamos con 2015->2019 y 2019->2022 y probamos
                           con 2022->2026, sin ver el futuro. Comparamos contra
                           dos referencias: repetir el crecimiento anterior y
                           usar el promedio.
    4. Proyeccion          reentrenamos con las tres transiciones y
                           proyectamos 2026 -> 2030.
    5. Potencial (US$)     restaurantes proyectados x kg de papa congelada
                           importada por restaurante x US$/kg (Comtrade).

El potencial es tamanio de mercado, no utilidad: no tenemos ventas, margenes
ni costos de McCain. Lo usamos para ordenar donde crecera mas la demanda.

Escribimos:
    data/processed/panel_municipios/      variables por municipio y version
    data/processed/potencial_metropolis/  una fila por metropoli
    reports/tables/modelo_metricas.csv    XGBoost contra las referencias
    reports/tables/potencial_metropolis.csv
    reports/tables/modelo_importancia.csv  ganancia de cada variable
    reports/figures/importancia_variables.png
    reports/figures/mapa_potencial.html   heatmap interactivo

Pipeline:
    DENUE + CONAPO -> panel por municipio (Spark) -> ejemplos (pandas)
        -> XGBoost -> proyeccion -> potencial por metropoli (US$) -> HTML
"""

import json
import sys
import time

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.utils.config import cargar_config
from src.utils.registro import configurar_registro
from src.utils.rutas import FIGURAS, PROCESSED, TABLAS, asegurar_carpetas
from src.utils.spark import cerrar_spark, crear_spark

log = configurar_registro("analysis")

SALIDAS = {
    "panel": PROCESSED / "panel_municipios",
    "potencial": PROCESSED / "potencial_metropolis",
    "metricas": TABLAS / "modelo_metricas.csv",
    "tabla": TABLAS / "potencial_metropolis.csv",
    "importancia": FIGURAS / "importancia_variables.png",
    "importancia_tabla": TABLAS / "modelo_importancia.csv",
    "mapa": FIGURAS / "mapa_potencial.html",
}

# Variables del modelo: todas medidas al inicio del periodo que predecimos.
VARIABLES = {
    "log_objetivo": "Restaurantes objetivo (log)",
    "objetivo_100k": "Restaurantes objetivo por 100 mil hab.",
    "prop_comida_rapida": "Proporción de comida rápida",
    "cadenas_por_restaurante": "Sucursales de cadenas por restaurante",
    "hoteles_100k": "Hoteles por 100 mil hab.",
    "bares_100k": "Bares por 100 mil hab.",
    "personal_promedio": "Personal promedio por restaurante",
    "crec_previo": "Crecimiento del periodo anterior (%)",
    "tasa_pob_2010_2020": "Crecimiento de población 2010–2020 (%)",
    "log_pib_per_capita": "PIB per cápita de la metrópoli (log)",
}


# ---------------------------------------------------------------------------
# 1. Panel por municipio (Spark)
# ---------------------------------------------------------------------------

def construir_panel(denue, municipios, categorias_objetivo):
    """Una fila por municipio metropolitano y version del DENUE."""
    es_objetivo = F.col("categoria").isin(categorias_objetivo)
    return (
        denue.join(F.broadcast(municipios.select("cve_mun5", "clave_metropoli")), "cve_mun5")
        .groupBy("cve_mun5", "clave_metropoli", "version", "fecha_corte")
        .agg(
            F.sum(es_objetivo.cast("int")).alias("objetivo"),
            F.sum((F.col("categoria") == "comida_rapida").cast("int")).alias("comida_rapida"),
            F.sum((es_objetivo & F.col("cadena").isNotNull()).cast("int")).alias("cadenas"),
            # Segmentos para la rentabilidad: cadenas aparte de los independientes.
            F.sum(((F.col("categoria") == "comida_rapida") & F.col("cadena").isNull())
                  .cast("int")).alias("comida_rapida_ind"),
            F.sum(((F.col("categoria") == "a_la_carta") & F.col("cadena").isNull())
                  .cast("int")).alias("a_la_carta_ind"),
            F.sum((F.col("categoria") == "hoteles").cast("int")).alias("hoteles"),
            F.sum((F.col("categoria") == "bares").cast("int")).alias("bares"),
            F.avg(F.when(es_objetivo, F.col("personal_min"))).alias("personal_promedio"),
            F.count("*").alias("sector_72"),
        )
    )


def centroides_metropolis(denue, municipios, version):
    """Latitud y longitud promedio de los establecimientos de cada metropoli."""
    return (
        denue.filter(F.col("version") == version)
        .filter(F.col("latitud").between(14, 33) & F.col("longitud").between(-119, -86))
        .join(F.broadcast(municipios.select("cve_mun5", "clave_metropoli")), "cve_mun5")
        .groupBy("clave_metropoli")
        .agg(F.avg("latitud").alias("latitud"), F.avg("longitud").alias("longitud"))
    )


# ---------------------------------------------------------------------------
# 2. Ejemplos: variables al inicio, crecimiento despues (pandas)
# ---------------------------------------------------------------------------

def tasa_anual(final, inicial, anios):
    """Crecimiento anual compuesto en %, o NaN si no se puede calcular."""
    final, inicial = np.asarray(final, float), np.asarray(inicial, float)
    with np.errstate(divide="ignore", invalid="ignore"):
        tasa = 100 * ((final / inicial) ** (1 / anios) - 1)
    return np.where((inicial > 0) & np.isfinite(tasa), tasa, np.nan)


def construir_ejemplos(panel, municipios, metropolis, versiones, cfg):
    """Una fila por municipio y transicion t -> t+1 (y una de proyeccion).

    versiones: lista ordenada de etiquetas del DENUE. La ultima transicion
    abierta (desde la version mas reciente) queda sin etiqueta: es la que
    proyectamos.
    """
    panel = panel.copy()
    panel["fecha_corte"] = pd.to_datetime(panel["fecha_corte"])
    ancho = panel.pivot_table(index="cve_mun5", columns="version",
                              values=["objetivo", "comida_rapida", "cadenas", "hoteles",
                                      "bares", "personal_promedio", "sector_72"],
                              aggfunc="first")
    fechas = panel.groupby("version")["fecha_corte"].first()

    base = municipios.set_index("cve_mun5")[["clave_metropoli", "poblacion_2020",
                                             "tasa_pob_2010_2020"]]
    pib = metropolis.set_index("clave_metropoli")["pib_per_capita"]

    filas = []
    for i, version in enumerate(versiones):
        if version not in fechas.index:
            continue
        siguiente = versiones[i + 1] if i + 1 < len(versiones) else None
        anterior = versiones[i - 1] if i > 0 else None

        def valor(columna, v):
            if v is None or (columna, v) not in ancho.columns:
                return pd.Series(np.nan, index=ancho.index)
            return ancho[(columna, v)]

        objetivo = valor("objetivo", version).fillna(0)
        df = pd.DataFrame(index=ancho.index)
        df["version_base"] = version
        df["fecha_base"] = fechas[version]
        df["objetivo_base"] = objetivo

        poblacion = base["poblacion_2020"].reindex(df.index)
        por_100k = 100_000 / poblacion.where(poblacion > 0)
        df["log_objetivo"] = np.log1p(objetivo)
        df["objetivo_100k"] = objetivo * por_100k
        df["prop_comida_rapida"] = valor("comida_rapida", version) / objetivo.where(objetivo > 0)
        df["cadenas_por_restaurante"] = valor("cadenas", version) / objetivo.where(objetivo > 0)
        df["hoteles_100k"] = valor("hoteles", version).fillna(0) * por_100k
        df["bares_100k"] = valor("bares", version).fillna(0) * por_100k
        df["personal_promedio"] = valor("personal_promedio", version)
        if anterior is not None and anterior in fechas.index:
            anios_prev = (fechas[version] - fechas[anterior]).days / 365.25
            df["crec_previo"] = tasa_anual(objetivo, valor("objetivo", anterior), anios_prev)
        else:
            df["crec_previo"] = np.nan
        df["tasa_pob_2010_2020"] = base["tasa_pob_2010_2020"].reindex(df.index)
        df["clave_metropoli"] = base["clave_metropoli"].reindex(df.index)
        df["log_pib_per_capita"] = np.log(df["clave_metropoli"].map(pib).astype(float))

        if siguiente is not None and siguiente in fechas.index:
            anios = (fechas[siguiente] - fechas[version]).days / 365.25
            df["version_objetivo"] = siguiente
            df["anios"] = anios
            crec = tasa_anual(valor("objetivo", siguiente).fillna(0), objetivo, anios)
            # Municipios con muy pocos restaurantes dan tasas de puro ruido.
            crec = np.where(objetivo >= cfg["min_restaurantes"], crec, np.nan)
            df["crecimiento"] = np.clip(crec, *cfg["limites_crecimiento"])
        else:
            df["version_objetivo"] = "proyeccion"
            df["anios"] = float(cfg["anios_proyeccion"])
            df["crecimiento"] = np.nan
        filas.append(df.reset_index())

    ejemplos = pd.concat(filas, ignore_index=True)
    # Un PIB o una poblacion en cero darian infinito: XGBoost lo rechaza, NaN no.
    ejemplos[list(VARIABLES)] = ejemplos[list(VARIABLES)].replace([np.inf, -np.inf], np.nan)
    return ejemplos[ejemplos["clave_metropoli"].notna()].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Entrenar y evaluar
# ---------------------------------------------------------------------------

def entrenar_xgboost(ejemplos, cfg):
    """XGBoost sobre los ejemplos con etiqueta. Pesamos por tamanio."""
    from xgboost import XGBRegressor

    datos = ejemplos.dropna(subset=["crecimiento"])
    modelo = XGBRegressor(**cfg["xgboost"])
    # Un municipio con 2,000 restaurantes pesa mas que uno con 12: sqrt amortigua.
    modelo.fit(datos[list(VARIABLES)], datos["crecimiento"],
               sample_weight=np.sqrt(datos["objetivo_base"]))
    return modelo


def metricas(real, predicho, pesos):
    error = np.asarray(predicho) - np.asarray(real)
    return {
        "mae": float(np.average(np.abs(error), weights=pesos)),
        "rmse": float(np.sqrt(np.average(error ** 2, weights=pesos))),
    }


def correlacion_rangos(a, b):
    """Spearman sin scipy: correlacion de Pearson entre los rangos."""
    return float(pd.Series(a).rank().corr(pd.Series(b).rank()))


def agregar_metropoli(datos, columna_crec):
    """Restaurantes al final del periodo por metropoli, con un crecimiento dado."""
    final = datos["objetivo_base"] * (1 + datos[columna_crec] / 100) ** datos["anios"]
    tabla = (datos.assign(final=final).groupby("clave_metropoli")
             .agg(inicial=("objetivo_base", "sum"), final=("final", "sum"),
                  anios=("anios", "first")))
    return pd.Series(tasa_anual(tabla["final"], tabla["inicial"], tabla["anios"]),
                     index=tabla.index)


def evaluar_modelo(ejemplos, cfg, version_prueba):
    """Validacion temporal: entrenamos con el pasado y probamos el ultimo periodo."""
    con_etiqueta = ejemplos.dropna(subset=["crecimiento"])
    entrenamiento = con_etiqueta[con_etiqueta["version_objetivo"] != version_prueba]
    prueba = con_etiqueta[con_etiqueta["version_objetivo"] == version_prueba].copy()
    if entrenamiento.empty or prueba.empty:
        raise ValueError("Se necesitan al menos tres versiones del DENUE para validar")

    modelo = entrenar_xgboost(entrenamiento, cfg)
    prueba["pred_xgboost"] = modelo.predict(prueba[list(VARIABLES)])
    # Referencia 1: el crecimiento sigue igual que el periodo anterior.
    prueba["pred_persistencia"] = prueba["crec_previo"].fillna(
        np.average(entrenamiento["crecimiento"], weights=np.sqrt(entrenamiento["objetivo_base"])))
    # Referencia 2: todos crecen al promedio del entrenamiento.
    prueba["pred_promedio"] = np.average(entrenamiento["crecimiento"],
                                         weights=np.sqrt(entrenamiento["objetivo_base"]))

    pesos = np.sqrt(prueba["objetivo_base"])
    real_metro = agregar_metropoli(prueba.assign(real=prueba["crecimiento"]), "real")
    filas = []
    for nombre, columna in [("XGBoost", "pred_xgboost"),
                            ("Persistencia", "pred_persistencia"),
                            ("Promedio", "pred_promedio")]:
        fila = {"modelo": nombre, **metricas(prueba["crecimiento"], prueba[columna], pesos)}
        pred_metro = agregar_metropoli(prueba, columna)
        comun = real_metro.dropna().index.intersection(pred_metro.dropna().index)
        fila["spearman_metropolis"] = (correlacion_rangos(real_metro[comun], pred_metro[comun])
                                       if nombre != "Promedio" else float("nan"))
        fila["mae_metropolis"] = float(np.mean(np.abs(pred_metro[comun] - real_metro[comun])))
        filas.append(fila)

    tabla = pd.DataFrame(filas)
    tabla["municipios_prueba"] = len(prueba)
    tabla["municipios_entrenamiento"] = len(entrenamiento)
    tabla["periodo_prueba"] = f"{prueba['version_base'].iloc[0]} -> {version_prueba}"
    return tabla, prueba


def contribuciones(modelo, datos):
    """Aporte de cada variable a cada prediccion (valores SHAP de XGBoost)."""
    import xgboost as xgb

    matriz = xgb.DMatrix(datos[list(VARIABLES)], feature_names=list(VARIABLES))
    aportes = modelo.get_booster().predict(matriz, pred_contribs=True)
    return pd.DataFrame(aportes[:, :-1], columns=list(VARIABLES), index=datos.index)


# ---------------------------------------------------------------------------
# 4 y 5. Proyeccion y potencial en dolares
# ---------------------------------------------------------------------------

def parametros_mercado(importaciones_anuales, objetivo_nacional):
    """kg de papa congelada importada por restaurante objetivo y US$/kg.

    Usamos el ultimo anio completo de Comtrade (12 meses en el pais principal).
    """
    completos = importaciones_anuales.groupby("anio")["meses"].max()
    anio = int(completos[completos >= 12].index.max())
    del_anio = importaciones_anuales[importaciones_anuales["anio"] == anio]
    kg, valor = float(del_anio["kg"].sum()), float(del_anio["valor_usd"].sum())
    return {
        "anio_importaciones": anio,
        "kg_importados": kg,
        "valor_importado_usd": valor,
        "usd_kg": valor / kg,
        "restaurantes_objetivo_nacional": int(objetivo_nacional),
        "kg_por_restaurante": kg / objetivo_nacional,
    }


def proyectar_potencial(modelo, ejemplos, indice, mercado, centroides):
    """Una fila por metropoli: restaurantes hoy y proyectados, y potencial US$."""
    futuro = ejemplos[ejemplos["version_objetivo"] == "proyeccion"].copy()
    futuro["crecimiento_pred"] = modelo.predict(futuro[list(VARIABLES)])
    futuro["objetivo_proyectado"] = (futuro["objetivo_base"]
                                     * (1 + futuro["crecimiento_pred"] / 100) ** futuro["anios"])

    aportes = contribuciones(modelo, futuro)
    peso = futuro["objetivo_base"]
    aportes_metro = (aportes.mul(peso, axis=0).groupby(futuro["clave_metropoli"]).sum()
                     .div(peso.groupby(futuro["clave_metropoli"]).sum(), axis=0))

    tabla = futuro.groupby("clave_metropoli").agg(
        restaurantes_hoy=("objetivo_base", "sum"),
        restaurantes_proyectados=("objetivo_proyectado", "sum"),
        anios=("anios", "first"),
        fecha_base=("fecha_base", "first"),
    )
    tabla["crecimiento_esperado"] = tasa_anual(tabla["restaurantes_proyectados"],
                                               tabla["restaurantes_hoy"], tabla["anios"])
    valor_restaurante = mercado["kg_por_restaurante"] * mercado["usd_kg"]
    tabla["potencial_hoy_usd"] = tabla["restaurantes_hoy"] * valor_restaurante
    tabla["potencial_proyectado_usd"] = tabla["restaurantes_proyectados"] * valor_restaurante
    tabla["potencial_incremental_usd"] = (tabla["potencial_proyectado_usd"]
                                          - tabla["potencial_hoy_usd"])
    tabla["anio_proyeccion"] = (pd.to_datetime(tabla["fecha_base"]).dt.year
                                + tabla["anios"].round().astype(int))

    # Las dos variables que mas empujan (o frenan) la prediccion de cada metropoli.
    def impulsores(fila):
        orden = fila.abs().sort_values(ascending=False).index[:2]
        return json.dumps([{"variable": VARIABLES[v], "aporte": round(float(fila[v]), 2)}
                           for v in orden], ensure_ascii=False)

    tabla["impulsores"] = aportes_metro.apply(impulsores, axis=1)

    columnas_indice = ["clave_metropoli", "metropoli", "tipo_metropoli", "ranking", "indice",
                       "crecimiento_anual", "cadenas_netas_100k", "hoteles_100k",
                       "poblacion_2020", "pib_per_capita"]
    tabla = (tabla.reset_index()
             .merge(indice[columnas_indice], on="clave_metropoli", how="left")
             .merge(centroides, on="clave_metropoli", how="left"))
    tabla["ranking_potencial"] = (tabla["potencial_incremental_usd"]
                                  .rank(ascending=False, method="first").astype(int))
    return tabla.sort_values("ranking_potencial").reset_index(drop=True), futuro


def validar_potencial(tabla, ejemplos, cfg):
    """Como validar_gold en clase: si algo no cuadra, no publicamos."""
    if tabla["clave_metropoli"].duplicated().any():
        raise ValueError("Potencial: hay metropolis repetidas")
    futuro = ejemplos[ejemplos["version_objetivo"] == "proyeccion"]
    if not np.isclose(tabla["restaurantes_hoy"].sum(), futuro["objetivo_base"].sum()):
        raise ValueError("Potencial: los restaurantes de hoy no suman lo del panel")
    diferencia = (tabla["potencial_proyectado_usd"] - tabla["potencial_hoy_usd"]
                  - tabla["potencial_incremental_usd"]).abs().max()
    if diferencia > 1:
        raise ValueError("Potencial: incremental != proyectado - hoy")
    bajo, alto = cfg["limites_crecimiento"]
    fuera = tabla[~tabla["crecimiento_esperado"].between(bajo - 1e-6, alto + 1e-6)]
    if len(fuera):
        raise ValueError(f"Potencial: crecimiento fuera de [{bajo}, {alto}] en {len(fuera)} metropolis")
    if (tabla["restaurantes_proyectados"] < 0).any():
        raise ValueError("Potencial: restaurantes proyectados negativos")
    return {"filas": len(tabla), "estado": "VALIDADO"}


def importancia_variables(modelo):
    ganancia = modelo.get_booster().get_score(importance_type="gain")
    total = sum(ganancia.values()) or 1
    return (pd.Series({VARIABLES[k]: v / total for k, v in ganancia.items()})
            .sort_values(ascending=False))


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    asegurar_carpetas()
    config = cargar_config()
    cfg = config["modelo"]
    versiones = [v["etiqueta"] for v in config["denue"]["versiones"]]
    spark = crear_spark("DatosMasivos-Modelo")
    inicio = time.perf_counter()

    rutas = {n: PROCESSED / n for n in ["denue_establecimientos", "metropolis_municipios",
                                        "metropolis", "indice_inversion",
                                        "importaciones_anuales"]}
    faltan = [str(r) for r in rutas.values() if not r.exists()]
    if faltan:
        log.error(f"No existe {faltan}. Primero: python -m src.analysis.indice_inversion")
        return 1

    denue = spark.read.parquet(str(rutas["denue_establecimientos"]))
    municipios = spark.read.parquet(str(rutas["metropolis_municipios"]))

    construir_panel(denue, municipios, config["denue"]["categorias_objetivo"]) \
        .write.mode("overwrite").parquet(str(SALIDAS["panel"]))
    panel = spark.read.parquet(str(SALIDAS["panel"])).toPandas()

    # Las tablas que siguen ya son chicas: pasamos a pandas.
    municipios_pd = municipios.toPandas()
    metropolis_pd = spark.read.parquet(str(rutas["metropolis"])).toPandas()
    indice_pd = spark.read.parquet(str(rutas["indice_inversion"])).toPandas()
    anual_pd = spark.read.parquet(str(rutas["importaciones_anuales"])).toPandas()
    version_final = versiones[-1]
    objetivo_nacional = (denue.filter(F.col("version") == version_final)
                         .filter(F.col("categoria").isin(config["denue"]["categorias_objetivo"]))
                         .count())
    centroides = centroides_metropolis(denue, municipios, version_final).toPandas()

    ejemplos = construir_ejemplos(panel, municipios_pd, metropolis_pd, versiones, cfg)
    tabla_metricas, _ = evaluar_modelo(ejemplos, cfg, version_final)
    tabla_metricas.to_csv(SALIDAS["metricas"], index=False)
    for _, fila in tabla_metricas.iterrows():
        log.info(f"{fila.modelo:12s} | MAE {fila.mae:5.2f} pp | RMSE {fila.rmse:5.2f} "
                 f"| Spearman metropolis {fila.spearman_metropolis:5.2f} "
                 f"| prueba {fila.periodo_prueba}")

    # Para proyectar usamos todas las transiciones con etiqueta.
    modelo = entrenar_xgboost(ejemplos, cfg)
    mercado = parametros_mercado(anual_pd, objetivo_nacional)
    log.info(f"Mercado {mercado['anio_importaciones']} | "
             f"{mercado['kg_por_restaurante']:,.0f} kg por restaurante | "
             f"US${mercado['usd_kg']:.2f}/kg")
    potencial, _ = proyectar_potencial(modelo, ejemplos, indice_pd,
                                       mercado, centroides)

    validar_potencial(potencial, ejemplos, cfg)
    potencial.to_csv(SALIDAS["tabla"], index=False, encoding="utf-8")
    spark.createDataFrame(potencial.drop(columns=["fecha_base"])) \
        .coalesce(1).write.mode("overwrite").parquet(str(SALIDAS["potencial"]))
    (TABLAS / "mercado_parametros.json").write_text(
        json.dumps(mercado, indent=2, ensure_ascii=False), encoding="utf-8")

    from src.analysis import graficas, mapa_potencial
    importancia = importancia_variables(modelo)
    importancia.rename("ganancia").rename_axis("variable").to_csv(
        SALIDAS["importancia_tabla"], encoding="utf-8")
    graficas.importancia_variables(importancia).savefig(
        SALIDAS["importancia"], dpi=150, facecolor=graficas.FONDO)
    mapa_potencial.generar(potencial, tabla_metricas, mercado, importancia, SALIDAS["mapa"])

    for _, fila in potencial.head(10).iterrows():
        log.info(f"{fila.ranking_potencial:2d}. {fila.metropoli:28s} "
                 f"+US${fila.potencial_incremental_usd / 1e6:6.2f} M "
                 f"| crecimiento esperado {fila.crecimiento_esperado:5.1f}%/anio")
    log.info(f"Modelo y potencial de {len(potencial)} metropolis en "
             f"{time.perf_counter() - inicio:.1f} s -> {SALIDAS['mapa']}")
    cerrar_spark(spark)
    return 0


if __name__ == "__main__":
    sys.exit(main())
