"""
Si McCain tuviera X millones, ¿donde los pondria? ESCENARIO SIMULADO.

El XGBoost dice cuanto crece cada mercado por si solo; no sabe nada de la
inversion de McCain. Para repartir un presupuesto hace falta una curva de
respuesta por ciudad, con tres ideas:

    penetracion   cada ciudad parte de su propia participacion de McCain (la
                  de sus segmentos, o la fijada a mano). Donde McCain ya
                  vende mucho, queda poco por ganar.
    tope          margen de contribucion extra maximo al anio = margen por kg x
                  ((techo_existente - participacion) x kg de hoy
                   + (techo_nuevo - participacion) x kg nuevos al 2030).
                  Solo segmentos que se ganan con inversion local (las cadenas
                  se negocian como cuenta nacional).
    curva         con la inversion "mitad" (costo_mitad x kg de 2030) se gana
                  la mitad del tope; con el doble, 75%; con el triple, 87.5%:
                  contribucion_extra(I) = tope x (1 - e^(-I/s)),  s = mitad / ln 2

Entrar a una ciudad cuesta un fijo mas un % de su mercado anual (barrera de
entrada: una ciudad grande pide mas vendedores, distribucion y promocion para
empezar). Reparto:

    1. Entre las ciudades elegidas, cada dolar va a donde rinde mas: se
       invierte hasta que el ultimo dolar rinda lo mismo en todas (lambda):
           I_i = max(0, s_i x ln(tope_i / (s_i x lambda)))
       con  sum(I_i) = X - suma de las entradas de las ciudades elegidas.
    2. Cuales ciudades: al rendimiento lambda, una ciudad vale la pena si lo
       que deja paga su entrada y su inversion a ese rendimiento. Su umbral es
       el lambda donde deja de valer la pena; ordenamos por umbral y probamos
       entrar a las primeras 1, 2, 3, ...; gana el grupo que mas deja.

Recuperacion: anios = (inversion + entrada) / contribucion extra al anio.
"""

import numpy as np
import pandas as pd


def capacidades(detalle, cfg):
    """Participacion actual, tope y escala de la curva de cada metropoli."""
    capturable = detalle[detalle["segmento"].isin(cfg["segmentos_capturables"])].copy()
    capturable["margen_x_kg"] = capturable["kg_mercado_proy"] * capturable["margen_usd_kg"]
    capturable["part_x_kg"] = capturable["kg_mercado_hoy"] * capturable["participacion_efectiva"]
    tabla = (capturable.groupby(["clave_metropoli", "metropoli"], dropna=False)
             .agg(kg_hoy=("kg_mercado_hoy", "sum"), kg_proy=("kg_mercado_proy", "sum"),
                  margen_x_kg=("margen_x_kg", "sum"), part_x_kg=("part_x_kg", "sum"),
                  costo_producto_usd_kg=("costo_producto_usd_kg", "first"))
             .reset_index())
    tabla["participacion_actual"] = np.where(tabla["kg_hoy"] > 0,
                                             tabla["part_x_kg"] / tabla["kg_hoy"], 0.0)
    tabla["margen_ponderado"] = np.where(tabla["kg_proy"] > 0,
                                         tabla["margen_x_kg"] / tabla["kg_proy"], 0.0)
    nuevos = (tabla["kg_proy"] - tabla["kg_hoy"]).clip(lower=0)
    hueco_existente = (cfg["techo_participacion_existente"] - tabla["participacion_actual"]).clip(lower=0)
    hueco_nuevo = (cfg["techo_participacion_nuevo"] - tabla["participacion_actual"]).clip(lower=0)
    tabla["tope_usd"] = (tabla["margen_ponderado"].clip(lower=0)
                         * (hueco_existente * tabla["kg_hoy"] + hueco_nuevo * nuevos))
    tabla["mitad_usd"] = np.maximum(cfg["costo_mitad_usd_por_kg"] * tabla["kg_proy"], 1.0)
    tabla["escala_usd"] = tabla["mitad_usd"] / np.log(2)
    tabla["rendimiento_primer_dolar"] = tabla["tope_usd"] / tabla["escala_usd"]
    # Barrera de entrada: un fijo mas un % del mercado anual de la ciudad.
    tabla["mercado_usd"] = tabla["kg_hoy"] * tabla["costo_producto_usd_kg"]
    tabla["entrada_usd"] = (cfg["costo_entrada_fijo_usd"]
                            + cfg["costo_entrada_pct_mercado"] * tabla["mercado_usd"])
    return tabla.drop(columns=["margen_x_kg", "part_x_kg", "costo_producto_usd_kg"])


def _repartir(tope, escala, presupuesto):
    """Reparto optimo sin costo de entrada: (inversiones, lambda)."""
    maximo = float(np.max(tope / escala)) if len(tope) else 0.0
    if presupuesto <= 0 or maximo <= 0:
        return np.zeros(len(tope)), maximo

    def a_lambda(lam):
        razon = tope / (escala * lam)
        return np.where(razon > 1, escala * np.log(np.maximum(razon, 1)), 0.0)

    bajo, alto = np.log(maximo) - 40, np.log(maximo)
    for _ in range(200):
        medio = (bajo + alto) / 2
        if a_lambda(np.exp(medio)).sum() > presupuesto:
            bajo = medio
        else:
            alto = medio
    inversion = a_lambda(np.exp(alto))
    total = inversion.sum()
    return (inversion * presupuesto / total if total > 0 else inversion), float(np.exp(alto))


def _umbral(tope, escala, entrada):
    """Rendimiento del ultimo dolar al que cada ciudad empieza a valer la pena.

    Al rendimiento lambda, entrar a una ciudad deja
        valor(lambda) = contribucion - lambda x (inversion + entrada),
    que baja al subir lambda. El umbral es el lambda donde valor = 0: mientras
    mas alto, antes conviene entrar a esa ciudad.
    """
    umbral = np.zeros(len(tope))
    for i in range(len(tope)):
        if tope[i] <= 0:
            continue
        tasa = tope[i] / escala[i]
        bajo, alto = np.log(tasa) - 40, np.log(tasa)
        for _ in range(100):
            lam = np.exp((bajo + alto) / 2)
            inv = escala[i] * np.log(tasa / lam)
            valor = tope[i] - lam * escala[i] - lam * (inv + entrada[i])
            if valor > 0:
                bajo = np.log(lam)
            else:
                alto = np.log(lam)
        umbral[i] = np.exp(bajo)
    return umbral


def asignar(cap, presupuesto, con_entrada=True):
    """Reparto optimo de un presupuesto con costo de entrada por ciudad.

    Ordenamos las ciudades por su umbral (antes conviene entrar a las de umbral
    mas alto), probamos entrar a las primeras 1, 2, 3, ... repartiendo lo que
    queda despues de sus entradas, y nos quedamos con las que mas contribucion
    extra dejan en total. Sin costo de entrada, conviene entrar a todas.
    """
    tabla = cap.copy().reset_index(drop=True)
    tope, escala = tabla["tope_usd"].to_numpy(), tabla["escala_usd"].to_numpy()
    entrada = (tabla["entrada_usd"].to_numpy() if con_entrada and "entrada_usd" in tabla
               else np.zeros(len(tabla)))
    umbral = _umbral(tope, escala, entrada)
    orden = [i for i in np.argsort(-umbral, kind="stable") if tope[i] > 0]
    mejor_total, mejor = -1.0, (np.array([], dtype=int), np.array([]))
    for k in range(1, len(orden) + 1):
        elegidas = np.array(orden[:k])
        disponible = presupuesto - entrada[elegidas].sum()
        if disponible <= 0:
            break
        inv, _ = _repartir(tope[elegidas], escala[elegidas], disponible)
        total = float((tope[elegidas] * (1 - np.exp(-inv / escala[elegidas]))).sum())
        if total > mejor_total + 1e-9:
            mejor_total, mejor = total, (elegidas, inv)
    variable = np.zeros(len(tabla))
    if len(mejor[0]):
        variable[mejor[0]] = mejor[1]
    entra = variable > 1e-6
    tabla["umbral_entrada"] = umbral
    tabla["inversion_variable_usd"] = variable
    tabla["costo_entrada_usd"] = np.where(entra, entrada, 0.0)
    tabla["inversion_usd"] = tabla["inversion_variable_usd"] + tabla["costo_entrada_usd"]
    alcanzado = 1 - np.exp(-variable / escala)
    tabla["pct_tope"] = alcanzado
    tabla["contribucion_extra_usd"] = tope * alcanzado
    tabla["anios_recuperacion"] = np.where(tabla["contribucion_extra_usd"] > 0,
                                           tabla["inversion_usd"] / tabla["contribucion_extra_usd"],
                                           np.nan)
    tabla["ranking_inversion"] = (tabla["inversion_usd"].rank(ascending=False, method="first")
                                  .astype(int))
    return tabla.sort_values("ranking_inversion").reset_index(drop=True)


def curva(cap, presupuestos, con_entrada=True):
    """Contribucion extra y recuperacion para varios presupuestos."""
    filas = []
    for x in presupuestos:
        reparto = asignar(cap, float(x), con_entrada)
        extra = float(reparto["contribucion_extra_usd"].sum())
        filas.append({"presupuesto_usd": float(x), "contribucion_extra_usd": extra,
                      "anios_recuperacion": x / extra if extra > 0 else np.nan,
                      "retorno_anual": extra / x if x > 0 else np.nan,
                      "ciudades": int((reparto["inversion_usd"] > 1).sum())})
    tabla = pd.DataFrame(filas)
    # Lo que rinde cada tramo adicional: muestra los rendimientos decrecientes.
    tabla["retorno_del_tramo"] = (tabla["contribucion_extra_usd"].diff()
                                  / tabla["presupuesto_usd"].diff())
    tabla.loc[0, "retorno_del_tramo"] = tabla.loc[0, "retorno_anual"]
    return tabla


def validar(reparto, presupuesto):
    invertido = reparto["inversion_usd"].sum()
    if presupuesto > 0 and reparto["tope_usd"].sum() > 0 and invertido > presupuesto * (1 + 1e-6):
        raise ValueError("Inversion: el reparto gasta mas que el presupuesto")
    if (reparto["contribucion_extra_usd"] > reparto["tope_usd"] + 1e-6).any():
        raise ValueError("Inversion: una ciudad rebasa su tope")
    if (reparto["inversion_usd"] < -1e-6).any():
        raise ValueError("Inversion: inversiones negativas")
    return {"filas": len(reparto), "estado": "VALIDADO"}
