"""
Si McCain tuviera X millones, ¿donde los pondria? ESCENARIO SIMULADO.

El XGBoost dice cuanto crece cada mercado por si solo; no sabe nada de la
inversion de McCain. Para repartir un presupuesto hace falta una curva de
respuesta: cuanto margen de contribucion extra deja cada dolar invertido en
una ciudad, con rendimientos decrecientes.

Curva por metropoli (supuestos editables en config, seccion inversion):

    tope   contribucion extra maxima al anio = margen por kg de la ciudad x
           (tope_existente x kg de hoy + tope_nuevo x kg nuevos al 2030).
           Solo segmentos que se ganan con inversion local (las cadenas se
           negocian como cuenta nacional).
    h      inversion que gana la mitad del tope = costo_mitad x kg de 2030.
           Con h se gana 50% del tope; con 2h, 75%; con 3h, 87.5%.

    contribucion_extra(I) = tope x (1 - 2^(-I/h)) = tope x (1 - e^(-I/s)),  s = h / ln 2

Reparto optimo de X: cada siguiente dolar va a la ciudad donde rinde mas.
Con curvas como esta, eso equivale a invertir en cada ciudad hasta que el
ultimo dolar rinda lo mismo en todas (lambda):

    I_i = max(0, s_i x ln(tope_i / (s_i x lambda)))      con  sum I_i = X

Recuperacion: anios = inversion / contribucion extra al anio.
"""

import numpy as np
import pandas as pd


def capacidades(detalle, cfg):
    """Tope y escala de la curva de cada metropoli."""
    capturable = detalle[detalle["segmento"].isin(cfg["segmentos_capturables"])].copy()
    capturable["margen_x_kg"] = capturable["kg_mercado_proy"] * capturable["margen_usd_kg"]
    tabla = (capturable.groupby(["clave_metropoli", "metropoli"], dropna=False)
             .agg(kg_hoy=("kg_mercado_hoy", "sum"), kg_proy=("kg_mercado_proy", "sum"),
                  margen_x_kg=("margen_x_kg", "sum"))
             .reset_index())
    tabla["margen_ponderado"] = np.where(tabla["kg_proy"] > 0,
                                         tabla["margen_x_kg"] / tabla["kg_proy"], 0.0)
    nuevos = (tabla["kg_proy"] - tabla["kg_hoy"]).clip(lower=0)
    tabla["tope_usd"] = (tabla["margen_ponderado"].clip(lower=0)
                         * (cfg["tope_participacion_existente"] * tabla["kg_hoy"]
                            + cfg["tope_participacion_nuevo"] * nuevos))
    tabla["mitad_usd"] = np.maximum(cfg["costo_mitad_usd_por_kg"] * tabla["kg_proy"], 1.0)
    tabla["escala_usd"] = tabla["mitad_usd"] / np.log(2)
    tabla["rendimiento_primer_dolar"] = tabla["tope_usd"] / tabla["escala_usd"]
    return tabla.drop(columns=["margen_x_kg"])


def _a_lambda(cap, lam):
    razon = cap["tope_usd"].to_numpy() / (cap["escala_usd"].to_numpy() * lam)
    inversion = np.where(razon > 1, cap["escala_usd"].to_numpy() * np.log(np.maximum(razon, 1)), 0.0)
    return inversion


def asignar(cap, presupuesto):
    """Reparto optimo de un presupuesto, con contribucion extra y recuperacion."""
    tabla = cap.copy()
    maximo = float(tabla["rendimiento_primer_dolar"].max()) if len(tabla) else 0.0
    if presupuesto <= 0 or maximo <= 0:
        tabla["inversion_usd"] = 0.0
    else:
        # Biseccion en escala logaritmica: el total invertido baja al subir lambda.
        bajo, alto = np.log(maximo) - 40, np.log(maximo)
        for _ in range(200):
            medio = (bajo + alto) / 2
            if _a_lambda(tabla, np.exp(medio)).sum() > presupuesto:
                bajo = medio
            else:
                alto = medio
        inversion = _a_lambda(tabla, np.exp(alto))
        total = inversion.sum()
        tabla["inversion_usd"] = inversion * (presupuesto / total) if total > 0 else 0.0
    alcanzado = 1 - np.exp(-tabla["inversion_usd"] / tabla["escala_usd"])
    tabla["pct_tope"] = alcanzado
    tabla["contribucion_extra_usd"] = tabla["tope_usd"] * alcanzado
    tabla["anios_recuperacion"] = np.where(tabla["contribucion_extra_usd"] > 0,
                                           tabla["inversion_usd"] / tabla["contribucion_extra_usd"],
                                           np.nan)
    tabla["ranking_inversion"] = (tabla["inversion_usd"].rank(ascending=False, method="first")
                                  .astype(int))
    return tabla.sort_values("ranking_inversion").reset_index(drop=True)


def curva(cap, presupuestos):
    """Contribucion extra y recuperacion para varios presupuestos."""
    filas = []
    for x in presupuestos:
        reparto = asignar(cap, float(x))
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
    if not np.isclose(reparto["inversion_usd"].sum(), presupuesto, rtol=1e-6) and presupuesto > 0 \
            and reparto["tope_usd"].sum() > 0:
        raise ValueError("Inversion: el reparto no suma el presupuesto")
    if (reparto["contribucion_extra_usd"] > reparto["tope_usd"] + 1e-6).any():
        raise ValueError("Inversion: una ciudad rebasa su tope")
    if (reparto["inversion_usd"] < -1e-6).any():
        raise ValueError("Inversion: inversiones negativas")
    return {"filas": len(reparto), "estado": "VALIDADO"}
