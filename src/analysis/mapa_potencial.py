"""
Heatmap interactivo del potencial de mercado por metropoli.

Es un entregable aparte del notebook: generamos un solo archivo HTML, sin
dependencias externas (los datos van dentro), que se abre en cualquier
navegador. Tiene tres piezas:

    mapa       silueta de Mexico y un punto por metropoli en su latitud/longitud;
               tamano = mercado proyectado, color = la metrica que elijas
    heatmap    una fila por metropoli y una columna por metrica, coloreada por
               percentil dentro de la columna
    detalle    al hacer clic en un punto o una fila: numeros e insights

Los insights se escriben aqui en Python a partir de los numeros, para que se
puedan probar y revisar en el repositorio.

Pipeline:
    potencial_metropolis (pandas) -> insights -> JSON -> plantilla HTML
"""

import json

import numpy as np
import pandas as pd


# Contorno de Mexico (Natural Earth 1:110m, dominio publico), en [lon, lat].
CONTORNO_MEXICO = [[-117.13,32.54],[-115.99,32.61],[-114.72,32.72],[-114.81,32.53],[-113.3,32.04],[-111.02,31.33],[-109.04,31.34],[-108.24,31.34],[-108.24,31.75],[-106.51,31.75],[-106.14,31.4],[-105.63,31.08],[-105.04,30.64],[-104.71,30.12],[-104.46,29.57],[-103.94,29.27],[-103.11,28.97],[-102.48,29.76],[-101.66,29.78],[-100.96,29.38],[-100.46,28.7],[-100.11,28.11],[-99.52,27.54],[-99.3,26.84],[-99.02,26.37],[-98.24,26.06],[-97.53,25.84],[-97.14,25.87],[-97.53,24.99],[-97.7,24.27],[-97.78,22.93],[-97.87,22.44],[-97.7,21.9],[-97.39,21.41],[-97.19,20.64],[-96.53,19.89],[-96.29,19.32],[-95.9,18.83],[-94.84,18.56],[-94.43,18.14],[-93.55,18.42],[-92.79,18.52],[-92.04,18.7],[-91.41,18.88],[-90.77,19.28],[-90.53,19.87],[-90.45,20.71],[-90.28,21.0],[-89.6,21.26],[-88.54,21.49],[-87.66,21.46],[-87.05,21.54],[-86.81,21.33],[-86.85,20.85],[-87.38,20.26],[-87.62,19.65],[-87.44,19.47],[-87.59,19.04],[-87.84,18.26],[-88.09,18.52],[-88.3,18.5],[-88.49,18.49],[-88.85,17.88],[-89.03,18.0],[-89.15,17.96],[-89.14,17.81],[-90.07,17.82],[-91.0,17.82],[-91.0,17.25],[-91.45,17.25],[-91.08,16.92],[-90.71,16.69],[-90.6,16.47],[-90.44,16.41],[-90.46,16.07],[-91.75,16.07],[-92.23,15.25],[-92.09,15.06],[-92.2,14.83],[-92.23,14.54],[-93.36,15.62],[-93.88,15.94],[-94.69,16.2],[-95.25,16.13],[-96.05,15.75],[-96.56,15.65],[-97.26,15.92],[-98.01,16.11],[-98.95,16.57],[-99.7,16.71],[-100.83,17.17],[-101.67,17.65],[-101.92,17.92],[-102.48,17.98],[-103.5,18.29],[-103.92,18.75],[-104.99,19.32],[-105.49,19.95],[-105.73,20.43],[-105.4,20.53],[-105.5,20.82],[-105.27,21.08],[-105.27,21.42],[-105.6,21.87],[-105.69,22.27],[-106.03,22.77],[-106.91,23.77],[-107.92,24.55],[-108.4,25.17],[-109.26,25.58],[-109.44,25.82],[-109.29,26.44],[-109.8,26.68],[-110.39,27.16],[-110.64,27.86],[-111.18,27.94],[-111.76,28.47],[-112.23,28.95],[-112.27,29.27],[-112.81,30.02],[-113.16,30.79],[-113.15,31.17],[-113.87,31.57],[-114.21,31.52],[-114.78,31.8],[-114.94,31.39],[-114.77,30.91],[-114.67,30.16],[-114.33,29.75],[-113.59,29.06],[-113.42,28.83],[-113.27,28.75],[-113.14,28.41],[-112.96,28.43],[-112.76,27.78],[-112.46,27.53],[-112.24,27.17],[-111.62,26.66],[-111.28,25.73],[-110.99,25.29],[-110.71,24.83],[-110.66,24.3],[-110.17,24.27],[-109.77,23.81],[-109.41,23.36],[-109.43,23.19],[-109.85,22.82],[-110.03,22.82],[-110.3,23.43],[-110.95,24.0],[-111.67,24.48],[-112.18,24.74],[-112.15,25.47],[-112.3,26.01],[-112.78,26.32],[-113.46,26.77],[-113.6,26.64],[-113.85,26.9],[-114.47,27.14],[-115.06,27.72],[-114.98,27.8],[-114.57,27.74],[-114.2,28.12],[-114.16,28.57],[-114.93,29.28],[-115.52,29.56],[-115.89,30.18],[-116.26,30.84],[-116.72,31.64],[-117.13,32.54]]


def millones(valor):
    return f"US${valor / 1e6:,.1f} M"


def nombre_corto(variable):
    """'Crecimiento de población 2010–2020 (%)' -> 'crecimiento de población 2010–2020'."""
    texto = variable.replace(" (%)", "").replace(" (log)", "")
    return texto if texto.startswith("PIB") else texto[0].lower() + texto[1:]


def insights(fila, tabla, mercado):
    """Frases cortas con los numeros que explican la posicion de una metropoli."""
    total = len(tabla)
    frases = [
        f"Puesto {int(fila.ranking_potencial)} de {total} en potencial incremental: "
        f"+{millones(fila.potencial_incremental_usd)} al {int(fila.anio_proyeccion)} "
        f"(de {millones(fila.potencial_hoy_usd)} a {millones(fila.potencial_proyectado_usd)})."
    ]

    mediana = tabla["crecimiento_esperado"].median()
    comparacion = "por encima" if fila.crecimiento_esperado > mediana else "por debajo"
    texto = (f"El modelo espera {fila.crecimiento_esperado:.1f}% anual de crecimiento en "
             f"restaurantes objetivo, {comparacion} de la mediana nacional "
             f"({mediana:.1f}%).")
    if pd.notna(fila.crecimiento_anual):
        texto += f" Entre 2019 y 2026 crecieron {fila.crecimiento_anual:.1f}% anual."
    frases.append(texto)

    nuevos = fila.restaurantes_proyectados - fila.restaurantes_hoy
    frases.append(f"Pasaría de {fila.restaurantes_hoy:,.0f} a "
                  f"{fila.restaurantes_proyectados:,.0f} restaurantes objetivo "
                  f"({nuevos:+,.0f}).")

    if pd.notna(fila.ranking):
        frases.append(f"En el índice de inversión ocupa el lugar {int(fila.ranking)} "
                      f"({fila.indice:.1f} de 100).")

    if pd.notna(fila.hoteles_100k) and fila.hoteles_100k >= tabla["hoteles_100k"].quantile(0.75):
        frases.append(f"Perfil turístico: {fila.hoteles_100k:.1f} hoteles por 100 mil "
                      f"habitantes, en el 25% más alto.")
    if (pd.notna(fila.cadenas_netas_100k)
            and fila.cadenas_netas_100k >= tabla["cadenas_netas_100k"].quantile(0.75)):
        frases.append(f"Las cadenas están abriendo: {fila.cadenas_netas_100k:+.1f} sucursales "
                      f"netas por 100 mil habitantes desde 2019.")

    impulsores = json.loads(fila.impulsores) if isinstance(fila.impulsores, str) else []
    if impulsores:
        partes = [f"{nombre_corto(i['variable'])} ({i['aporte']:+.1f} pp)" for i in impulsores]
        frases.append("Lo que más mueve su predicción: " + " y ".join(partes) + ".")
    return frases


def preparar_datos(tabla, mercado):
    filas = []
    for fila in tabla.itertuples(index=False):
        def num(valor, decimales=2):
            return None if pd.isna(valor) else round(float(valor), decimales)
        filas.append({
            "clave": fila.clave_metropoli,
            "nombre": fila.metropoli if isinstance(fila.metropoli, str) else fila.clave_metropoli,
            "tipo": fila.tipo_metropoli if isinstance(fila.tipo_metropoli, str) else "",
            "lat": num(fila.latitud, 4), "lon": num(fila.longitud, 4),
            "rank_potencial": int(fila.ranking_potencial),
            "rank_indice": None if pd.isna(fila.ranking) else int(fila.ranking),
            "indice": num(fila.indice, 1),
            "crec_hist": num(fila.crecimiento_anual, 2),
            "crec_esp": num(fila.crecimiento_esperado, 2),
            "rest_hoy": num(fila.restaurantes_hoy, 0),
            "rest_proy": num(fila.restaurantes_proyectados, 0),
            "pot_hoy": num(fila.potencial_hoy_usd, 0),
            "pot_proy": num(fila.potencial_proyectado_usd, 0),
            "pot_inc": num(fila.potencial_incremental_usd, 0),
            "cadenas_100k": num(fila.cadenas_netas_100k, 2),
            "hoteles_100k": num(fila.hoteles_100k, 2),
            "poblacion": num(fila.poblacion_2020, 0),
            "insights": insights(fila, tabla, mercado),
        })
    return filas


def datos_rentabilidad(rentabilidad):
    """Insumos crudos del escenario: el HTML recalcula todo en vivo con ellos.

    Por metropoli van los locales de cada segmento hoy y proyectados (dato
    real + XGBoost); en el contexto, los supuestos editables. Asi, al mover un
    supuesto en la pagina, la contribucion y el reparto se recalculan igual
    que en src/analysis/rentabilidad.py e inversion.py.
    """
    detalle = rentabilidad["detalle"]
    por_metro = {}
    for clave, grupo in detalle.groupby("clave_metropoli"):
        por_metro[clave] = {s.segmento: [round(float(s.locales_hoy), 2),
                                         round(float(s.locales_proyectados), 2)]
                            for s in grupo.itertuples()}
    g = rentabilidad["generales"]
    mt = rentabilidad["metricas"]
    todos = mt[mt["segmento"] == "Todos"].set_index("modelo")["mae"]
    supuestos = {
        "precio_lista": g["precio_lista_usd_kg"],
        "costo_producto": g["costo_producto_usd_kg"],
        "costo_importacion": g["costo_importacion_usd_kg"],
        "part_ciudad": dict(g.get("participacion_por_ciudad") or {}),
        "kg_importados": g["kg_importados"],
        "segmentos": [{"clave": s.segmento, "nombre": s.nombre, "locales_pais": int(s.locales_pais),
                       "consumo_relativo": float(s.consumo_relativo), "descuento": float(s.descuento),
                       "costo_servir": float(s.costo_servir_usd_kg),
                       "participacion": float(s.participacion)}
                      for s in rentabilidad["matriz"].itertuples()],
    }
    cfg_inv = rentabilidad.get("cfg_inversion")
    if cfg_inv:
        supuestos.update({
            "presupuesto": float(cfg_inv["presupuesto_usd"]),
            "techo_existente": cfg_inv["techo_participacion_existente"],
            "techo_nuevo": cfg_inv["techo_participacion_nuevo"],
            "costo_mitad": cfg_inv["costo_mitad_usd_por_kg"],
            "entrada_fija": float(cfg_inv["costo_entrada_fijo_usd"]),
            "entrada_pct": float(cfg_inv["costo_entrada_pct_mercado"]),
            "capturables": list(cfg_inv["segmentos_capturables"]),
            "curva": [float(x) for x in cfg_inv["curva_presupuestos_usd"]],
        })
    contexto = {"supuestos": supuestos, "ganador": rentabilidad["ganador"],
                "mae": {k: round(float(v), 2) for k, v in todos.items()},
                "anio_importaciones": g["anio_importaciones"]}
    return por_metro, contexto


def generar(tabla, metricas, mercado, importancia, ruta=None, rentabilidad=None):
    """Regresamos el HTML; si damos ruta, ademas lo escribimos.

    rentabilidad (opcional): insumos del escenario de src.analysis.rentabilidad
    (detalle, matriz, generales, metricas, ganador y cfg_inversion). Si viene,
    la pagina agrega las vistas Rentabilidad e Inversion, con los supuestos
    editables y todo recalculado en vivo.
    """
    xgb = metricas.set_index("modelo")
    anio = int(tabla["anio_proyeccion"].mode().iloc[0])
    contexto = {
        "anio": anio,
        "kg_por_restaurante": round(mercado["kg_por_restaurante"], 1),
        "usd_kg": round(mercado["usd_kg"], 2),
        "anio_importaciones": mercado["anio_importaciones"],
        "total_inc": float(tabla["potencial_incremental_usd"].sum()),
        "mae": {m: round(float(xgb.loc[m, "mae"]), 2) for m in xgb.index},
        "spearman": {m: (None if np.isnan(xgb.loc[m, "spearman_metropolis"])
                         else round(float(xgb.loc[m, "spearman_metropolis"]), 2))
                     for m in xgb.index},
        "periodo_prueba": str(metricas["periodo_prueba"].iloc[0]),
        "importancia": [{"variable": k, "valor": round(float(v), 3)}
                        for k, v in importancia.head(5).items()],
    }
    metros = preparar_datos(tabla, mercado)
    if rentabilidad is not None:
        por_metro, contexto["rentabilidad"] = datos_rentabilidad(rentabilidad)
        for m in metros:
            m["seg"] = por_metro.get(m["clave"], {})
    datos = json.dumps({"metros": metros, "contexto": contexto,
                        "contorno": CONTORNO_MEXICO},
                       ensure_ascii=False).replace("</", "<\\/")
    html = PLANTILLA.replace("/*__DATOS__*/null", datos)
    if ruta is not None:
        ruta.write_text(html, encoding="utf-8")
    return html


PLANTILLA = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Potencial por metrópoli</title>
<style>
:root{
  --bg:#fcfcfb; --surface:#ffffff; --ink:#0b0b0b; --ink2:#52514e; --muted:#8a8984;
  --grid:#e6e5e1; --line:#d9d8d4; --accent:#2a78d6; --sel:#eb6834;
  --r0:#eef4fc; --r1:#1a4f94; --tierra:#f1f0ec;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    --bg:#111214; --surface:#1a1b1e; --ink:#f2f2f0; --ink2:#b3b2ad; --muted:#85847f;
    --grid:#2a2b2f; --line:#34353a; --accent:#5b9be6; --sel:#f08a5d;
    --r0:#1c2633; --r1:#7fb3f0; --tierra:#222327;
  }
}
:root[data-theme="dark"]{
  --bg:#111214; --surface:#1a1b1e; --ink:#f2f2f0; --ink2:#b3b2ad; --muted:#85847f;
  --grid:#2a2b2f; --line:#34353a; --accent:#5b9be6; --sel:#f08a5d;
  --r0:#1c2633; --r1:#7fb3f0; --tierra:#222327;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
  font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding:24px 16px 40px}
h1{font-size:22px;margin:0 0 6px;letter-spacing:-.01em}
.sub{color:var(--ink2);margin:0 0 18px;max-width:820px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:16px}
.kpi{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.kpi b{display:block;font-size:20px;font-variant-numeric:tabular-nums}
.kpi span{color:var(--ink2);font-size:12px}
.controles{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px}
.controles input,.controles select{font:inherit;color:var(--ink);background:var(--surface);
  border:1px solid var(--line);border-radius:8px;padding:6px 10px}
.controles input{flex:1 1 200px;min-width:0}
.grid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(0,1fr);gap:14px}
@media (max-width:820px){.grid{grid-template-columns:1fr}}
.panel{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.vistas{display:inline-flex;gap:2px;padding:3px;margin:0 0 14px;background:var(--surface);
  border:1px solid var(--line);border-radius:10px;flex-wrap:wrap}
.vistas button{font:inherit;font-size:13px;color:var(--ink2);background:transparent;border:0;
  border-radius:7px;padding:7px 14px;cursor:pointer}
.vistas button[aria-selected="true"]{background:var(--accent);color:#fff;font-weight:600}
#supuestos,#inversion{margin:0 0 14px}
.panel h2 .boton{float:right;margin-top:-4px}
.generales{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px;margin:4px 0 12px}
.generales .tipo{display:block;margin-bottom:4px}
.campo{display:inline-flex;align-items:center;gap:4px;color:var(--ink2);font-size:12px}
.campo input{width:84px;font:inherit;font-size:13px;color:var(--ink);background:var(--bg);
  border:1px solid var(--line);border-radius:7px;padding:5px 7px;text-align:right}
.campo.grande input{width:110px;font-size:15px;font-weight:600}
.campo input:focus{outline:2px solid var(--accent);outline-offset:1px}
.desliza{overflow-x:auto}
table.matriz{font-size:12.5px;margin-bottom:6px}
table.matriz th,table.matriz td{background:none;border-bottom:1px solid var(--grid);border-radius:0;cursor:default}
table.matriz tr{cursor:default}
.tipo{color:var(--ink2);font-size:12px}
#detalle h3{font-size:13px;margin:14px 0 6px;color:var(--ink2)}
td.editable{padding:2px 4px} td.editable input{width:64px}
td.editable.fija input{border-color:var(--sel);font-weight:600}
.cifra{display:block;font-size:18px;padding-top:4px}
#detalle dl dt{min-width:0} #detalle .frase{margin:0 0 8px;font-size:13px}
.panel h2{font-size:14px;margin:0 0 8px}
svg text{fill:var(--ink2);font-size:11px}
.punto{cursor:pointer;stroke:var(--surface);stroke-width:1.5}
svg text.etiqueta{fill:var(--ink);font-weight:600;font-size:11.5px;paint-order:stroke;
  stroke:var(--surface);stroke-width:3.5px;stroke-linejoin:round;pointer-events:none}
.punto.sel{stroke:var(--sel);stroke-width:3}
.leyenda{display:flex;align-items:center;gap:8px;color:var(--ink2);font-size:12px;margin-top:6px}
.rampa{height:10px;width:140px;border-radius:5px;background:linear-gradient(90deg,var(--r0),var(--r1))}
#detalle .nombre{font-size:18px;font-weight:600;margin:0}
#detalle .tipo{color:var(--ink2);font-size:12px;margin:0 0 10px}
#detalle dl{display:grid;grid-template-columns:1fr auto;gap:4px 12px;margin:0 0 10px}
#detalle dt{color:var(--ink2)} #detalle dd{margin:0;text-align:right;font-variant-numeric:tabular-nums}
#detalle ul{margin:0;padding-left:18px} #detalle li{margin:4px 0}
.tabla{overflow-x:auto;margin-top:14px}
table{border-collapse:separate;border-spacing:2px;width:100%;font-size:12.5px}
th{position:sticky;top:0;background:var(--surface);color:var(--ink2);font-weight:600;
  text-align:right;padding:6px 8px;cursor:pointer;white-space:nowrap}
th:first-child,td:first-child,th.izq,td.nombre{text-align:left}
td{padding:6px 8px;text-align:right;border-radius:4px;font-variant-numeric:tabular-nums;white-space:nowrap}
table.seg{margin:10px 0 4px;font-size:12px}
table.seg th,table.seg td{padding:4px 6px;background:none;border-bottom:1px solid var(--grid);border-radius:0}
tr{cursor:pointer} tr.sel td:first-child{box-shadow:inset 3px 0 0 var(--sel)}
td.nombre{font-weight:500;color:var(--ink)}
.nota{color:var(--muted);font-size:12px;margin-top:14px}
#tip{position:fixed;pointer-events:none;background:var(--surface);border:1px solid var(--line);
  border-radius:8px;padding:6px 9px;font-size:12px;box-shadow:0 4px 14px rgba(0,0,0,.12);display:none;z-index:5}
.boton{font:inherit;font-size:12px;color:var(--ink2);background:transparent;border:1px solid var(--line);
  border-radius:8px;padding:6px 10px;cursor:pointer}
</style>
</head>
<body>
<div class="wrap">
  <h1>¿Dónde crecerá el mercado de papas a la francesa?</h1>
  <p class="sub" id="sub"></p>
  <div class="vistas" id="vistas" role="tablist" aria-label="Vista"></div>
  <div class="kpis" id="kpis"></div>
  <div class="panel" id="supuestos" hidden></div>
  <div class="panel" id="inversion" hidden></div>
  <div class="controles">
    <input id="buscar" type="search" placeholder="Buscar metrópoli…" aria-label="Buscar metrópoli">
    <select id="metrica" aria-label="Color del mapa"></select>
    <select id="top" aria-label="Metrópolis en la tabla">
      <option value="999" selected>Todas</option><option value="20">Top 20</option><option value="40">Top 40</option>
    </select>
    <button class="boton" id="tema" type="button">Tema</button>
  </div>
  <div class="grid">
    <div class="panel">
      <h2>Mapa: tamaño = mercado proyectado, color = métrica elegida</h2>
      <svg id="mapa" viewBox="0 0 640 420" role="img" aria-label="Mapa de metrópolis"></svg>
      <div class="leyenda"><span id="leyMin"></span><div class="rampa"></div><span id="leyMax"></span></div>
    </div>
    <div class="panel" id="detalle" aria-live="polite"></div>
  </div>
  <div class="panel tabla">
    <h2 id="tituloTabla">Heatmap: cada celda coloreada por su percentil dentro de la columna</h2>
    <table id="heat"><thead></thead><tbody></tbody></table>
  </div>
  <p class="nota" id="nota"></p>
</div>
<div id="tip"></div>
<script>
const D = /*__DATOS__*/null;
const C = D.contexto;
const R = C.rentabilidad || null;
const fmtM = v => v == null || isNaN(v) ? "—" : "US$" + (v / 1e6).toLocaleString("es-MX", {minimumFractionDigits: 1, maximumFractionDigits: 1}) + " M";
const fmtD = v => v == null || isNaN(v) ? "—" : (Math.abs(v) < 1e6 ? "US$" + Math.round(v / 1e3).toLocaleString("es-MX") + " mil" : fmtM(v));
const fmtN = v => v == null || isNaN(v) ? "—" : Math.round(v).toLocaleString("es-MX");
const fmtP = v => v == null || isNaN(v) ? "—" : v.toLocaleString("es-MX", {minimumFractionDigits: 1, maximumFractionDigits: 1}) + "%";
const fmt1 = v => v == null || isNaN(v) ? "—" : v.toLocaleString("es-MX", {minimumFractionDigits: 1, maximumFractionDigits: 1});
const fmtA = v => v == null || isNaN(v) || !isFinite(v) ? "—" : fmt1(v);
const suma = a => a.reduce((x, y) => x + y, 0);

// ---------------------------------------------------------------------------
// Escenario: supuestos editables y calculo en vivo (las mismas formulas que
// src/analysis/rentabilidad.py e inversion.py).
// ---------------------------------------------------------------------------
const ORIGINAL = R ? JSON.stringify(R.supuestos) : null;
let S = R ? JSON.parse(ORIGINAL) : null;
if (S) S.part_ciudad = S.part_ciudad || {};
const tieneInversion = () => S && S.presupuesto != null;

function repartir(ms, X) {
  // Sin costo de entrada: invertir hasta que el ultimo dolar rinda lo mismo en todas.
  const inv = lam => ms.map(m => m.tope / (m.escala * lam) > 1 ? m.escala * Math.log(m.tope / (m.escala * lam)) : 0);
  const max = Math.max(...ms.map(m => m.tope / m.escala));
  if (!(max > 0) || X <= 0) return {I: ms.map(() => 0), lam: max};
  let bajo = Math.log(max) - 40, alto = Math.log(max);
  for (let i = 0; i < 120; i++) { const med = (bajo + alto) / 2; if (suma(inv(Math.exp(med))) > X) bajo = med; else alto = med; }
  let I = inv(Math.exp(alto)); const t = suma(I); if (t > 0) I = I.map(v => v * X / t);
  return {I, lam: Math.exp(alto)};
}

function umbral(m) {
  // Rendimiento del ultimo dolar al que la ciudad empieza a valer la pena:
  // valor(lam) = contribucion - lam * (inversion + entrada) = 0.
  const tasa = m.tope / m.escala, F = m.entrada || 0;
  let bajo = Math.log(tasa) - 40, alto = Math.log(tasa);
  for (let i = 0; i < 100; i++) {
    const lam = Math.exp((bajo + alto) / 2), inv = m.escala * Math.log(tasa / lam);
    if (m.tope - lam * m.escala - lam * (inv + F) > 0) bajo = Math.log(lam); else alto = Math.log(lam);
  }
  return Math.exp(bajo);
}

function asignar(ms, X) {
  // Ordenamos por umbral y probamos entrar a las primeras 1, 2, 3...;
  // gana el grupo que mas contribucion extra deja.
  const orden = ms.filter(m => m.tope > 0 && m.escala > 0)
    .map(m => ({m, u: umbral(m)})).sort((a, b) => b.u - a.u).map(x => x.m);
  let mejor = {total: -1, elegidas: [], I: []};
  for (let k = 1; k <= orden.length; k++) {
    const elegidas = orden.slice(0, k);
    const disponible = X - suma(elegidas.map(m => m.entrada || 0));
    if (disponible <= 0) break;
    const r = repartir(elegidas, disponible);
    const total = suma(elegidas.map((m, j) => m.tope * (1 - Math.exp(-r.I[j] / m.escala))));
    if (total > mejor.total + 1e-9) mejor = {total, elegidas, I: r.I};
  }
  const salida = new Map();
  mejor.elegidas.forEach((m, k) => {
    const v = mejor.I[k] || 0;
    if (v <= 1e-6) return;
    const pct = 1 - Math.exp(-v / m.escala), extra = m.tope * pct, total = v + (m.entrada || 0);
    salida.set(m.clave, {inv: total, variable: v, extra, pct, anios: extra > 0 ? total / extra : null});
  });
  return salida;
}

function recalcular() {
  if (!R) return;
  const segs = S.segmentos;
  const unidades = suma(segs.map(s => s.locales_pais * s.consumo_relativo));
  const kgUnidad = unidades > 0 ? S.kg_importados / unidades : 0;
  segs.forEach(s => {
    s.kg_local = s.consumo_relativo * kgUnidad;
    s.neto = S.precio_lista * (1 - s.descuento);
    s.margen_kg = s.neto - S.costo_producto - S.costo_importacion - s.costo_servir;
    s.margen_pct = s.neto > 0 ? s.margen_kg / s.neto : 0;
  });
  const capturables = new Set(S.capturables || []);
  for (const m of D.metros) {
    // Penetracion: la de cada segmento; si la ciudad trae una fija, escalamos
    // sus segmentos para que el promedio ponderado por kg de hoy sea esa.
    let kgTot = 0, kgPart = 0;
    for (const s of segs) { const [h] = (m.seg && m.seg[s.clave]) || [0, 0]; kgTot += h * s.kg_local; kgPart += h * s.kg_local * s.participacion; }
    const mezcla = kgTot > 0 ? kgPart / kgTot : 0;
    const fija = S.part_ciudad && S.part_ciudad[m.clave] != null ? S.part_ciudad[m.clave] : null;
    const factor = fija != null && mezcla > 0 ? fija / mezcla : 1;
    m.part_actual = 100 * Math.min(1, mezcla * factor);
    m.part_fija = fija != null;
    let vh = 0, vp = 0, uh = 0, up = 0, kch = 0, kcp = 0, mxk = 0, pxk = 0;
    m.segmentos = [];
    for (const s of segs) {
      const [h, p] = (m.seg && m.seg[s.clave]) || [0, 0];
      const kgh = h * s.kg_local, kgp = p * s.kg_local;
      const part = Math.min(1, Math.max(0, s.participacion * factor));
      vh += kgh * part * s.neto; vp += kgp * part * s.neto;
      const uhs = kgh * part * s.margen_kg, ups = kgp * part * s.margen_kg;
      uh += uhs; up += ups;
      m["u_" + s.clave] = ups;
      m.segmentos.push({nombre: s.nombre, hoy: h, proy: p, util: ups, part});
      if (capturables.has(s.clave)) { kch += kgh; kcp += kgp; mxk += kgp * s.margen_kg; pxk += kgh * part; }
    }
    m.segmentos.sort((a, b) => b.util - a.util);
    Object.assign(m, {util_hoy: uh, util_proy: up, util_inc: up - uh, ventas_proy: vp,
                      margen: vp > 0 ? 100 * up / vp : null});
    if (tieneInversion()) {
      const margenPond = kcp > 0 ? mxk / kcp : 0;
      const pCap = kch > 0 ? pxk / kch : 0;
      m.hueco = 100 * Math.max(S.techo_nuevo - pCap, 0);
      m.tope = Math.max(margenPond, 0) * (Math.max(S.techo_existente - pCap, 0) * kch
                                         + Math.max(S.techo_nuevo - pCap, 0) * Math.max(kcp - kch, 0));
      m.mitad = Math.max(S.costo_mitad * kcp, 1);
      m.mercado = kch * S.costo_producto;
      m.entrada = (S.entrada_fija || 0) + (S.entrada_pct || 0) * m.mercado;
      m.escala = m.mitad / Math.LN2;
    }
  }
  if (tieneInversion()) {
    const reparto = asignar(D.metros, S.presupuesto);
    for (const m of D.metros) {
      const r = reparto.get(m.clave) || {inv: 0, extra: 0, pct: 0, anios: null};
      Object.assign(m, {inv: r.inv, extra: r.extra, pct_tope: 100 * r.pct, anios: r.inv > 1 ? r.anios : null});
    }
  }
  rangos("util_proy", "rank_util"); rangos("inv", "rank_inv");
}

function rangos(k, destino) {
  [...D.metros].sort((a, b) => (b[k] || 0) - (a[k] || 0)).forEach((m, i) => m[destino] = i + 1);
}

// ---------------------------------------------------------------------------
// Vistas
// ---------------------------------------------------------------------------
const VISTAS = {
  mercado: {
    nombre: "Mercado", rank: "rank_potencial",
    metricas: [["pot_inc", "Color: potencial incremental (US$)"], ["crec_esp", "Color: crecimiento esperado (%)"], ["indice", "Color: índice de inversión"]],
    columnas: () => [
      {k: "rank_potencial", t: "#"}, {k: "nombre", t: "Metrópoli"},
      {k: "pot_inc", t: "Potencial incremental", f: fmtM, heat: true},
      {k: "pot_proy", t: "Mercado " + C.anio, f: fmtM, heat: true},
      {k: "crec_esp", t: "Crec. esperado", f: fmtP, heat: true},
      {k: "crec_hist", t: "Crec. 2019–26", f: fmtP, heat: true},
      {k: "rest_hoy", t: "Restaurantes hoy", f: fmtN, heat: true},
      {k: "cadenas_100k", t: "Cadenas netas/100k", f: fmt1, heat: true},
      {k: "hoteles_100k", t: "Hoteles/100k", f: fmt1, heat: true},
      {k: "indice", t: "Índice", f: fmt1, heat: true}],
  },
  rentabilidad: {
    nombre: "Rentabilidad*", rank: "rank_util",
    metricas: [["util_proy", "Color: contribución " + C.anio], ["util_inc", "Color: contribución nueva"], ["margen", "Color: margen %"], ["part_actual", "Color: participación actual"]],
    columnas: () => [
      {k: "rank_util", t: "#"}, {k: "nombre", t: "Metrópoli"},
      {k: "util_proy", t: "Contribución " + C.anio, f: fmtD, heat: true},
      {k: "util_inc", t: "Contribución nueva", f: fmtD, heat: true},
      {k: "ventas_proy", t: "Ventas McCain " + C.anio, f: fmtM, heat: true},
      {k: "margen", t: "Margen", f: fmtP, heat: true},
      {k: "part_actual", t: "Participación actual", f: fmtP, heat: true},
      ...S.segmentos.map(s => ({k: "u_" + s.clave, t: s.nombre.replace(" (cuenta nacional)", "").replace(" (contrato)", ""), f: fmtD, heat: true})),
      {k: "crec_esp", t: "Crec. esperado", f: fmtP, heat: true}],
  },
  inversion: {
    nombre: "Inversión*", rank: "rank_inv",
    metricas: [["inv", "Color: inversión asignada"], ["extra", "Color: contribución extra al año"], ["hueco", "Color: hueco hasta el techo"], ["part_actual", "Color: participación actual"]],
    columnas: () => [
      {k: "rank_inv", t: "#"}, {k: "nombre", t: "Metrópoli"},
      {k: "part_actual", t: "Participación actual (editable)", editable: true},
      {k: "inv", t: "Inversión (con entrada)", f: fmtD, heat: true},
      {k: "entrada", t: "Costo de entrada", f: fmtD, heat: true},
      {k: "extra", t: "Contribución extra al año", f: fmtD, heat: true},
      {k: "anios", t: "Años para recuperar", f: fmtA, heat: false},
      {k: "pct_tope", t: "% del hueco ganado", f: fmtP, heat: true},
      {k: "hueco", t: "Hueco hasta el techo (pts)", f: fmt1, heat: true},
      {k: "util_proy", t: "Contribución " + C.anio, f: fmtD, heat: true},
      {k: "crec_esp", t: "Crec. esperado", f: fmtP, heat: true}],
  },
};
let vista = "mercado";
let seleccion = D.metros[0] ? D.metros[0].clave : null;
let orden = {k: "rank_potencial", asc: true};
let pct = {};

function columnas() { return VISTAS[vista].columnas(); }

function percentiles() {
  pct = {};
  const claves = new Set([...columnas().filter(c => c.heat).map(c => c.k), ...VISTAS[vista].metricas.map(([k]) => k)]);
  claves.forEach(k => {
    const vals = D.metros.map(m => m[k]).filter(v => v != null && !isNaN(v)).sort((a, b) => a - b);
    pct[k] = v => {
      if (v == null || isNaN(v) || vals.length < 2 || vals[0] === vals[vals.length - 1]) return null;
      let i = 0; while (i < vals.length && vals[i] < v) i++;
      return i / (vals.length - 1);
    };
  });
}

function rampa(t) {
  const css = getComputedStyle(document.documentElement);
  const a = hex(css.getPropertyValue("--r0")), b = hex(css.getPropertyValue("--r1"));
  const c = a.map((x, i) => Math.round(x + (b[i] - x) * t));
  return `rgb(${c.join(",")})`;
}
function hex(h) { h = h.trim().replace("#", ""); return [0, 2, 4].map(i => parseInt(h.slice(i, i + 2), 16)); }
// Texto negro o blanco segun la luminancia del fondo de la celda.
function tinta(t) {
  const c = rampa(t).match(/\d+/g).map(Number);
  const lum = (0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]) / 255;
  return lum > 0.55 ? "#0b0b0b" : "#ffffff";
}

function selectorVistas() {
  const caja = document.getElementById("vistas");
  if (!R) { caja.hidden = true; return; }
  const lista = Object.entries(VISTAS).filter(([k]) => k !== "inversion" || tieneInversion());
  caja.innerHTML = lista.map(([k, v]) => `<button type="button" role="tab" data-v="${k}" aria-selected="${k === vista}">${v.nombre}</button>`).join("");
  caja.querySelectorAll("button").forEach(b => b.addEventListener("click", () => cambiarVista(b.dataset.v)));
}

function cambiarVista(v) {
  vista = v;
  orden = {k: VISTAS[v].rank, asc: true};
  document.getElementById("metrica").innerHTML = VISTAS[v].metricas.map(([k, t]) => `<option value="${k}">${t}</option>`).join("");
  document.getElementById("supuestos").hidden = v === "mercado";
  document.getElementById("inversion").hidden = v !== "inversion";
  selectorVistas(); refrescar();
}

// ---------------------------------------------------------------------------
// Encabezado, KPIs y nota
// ---------------------------------------------------------------------------
function encabezado() {
  document.getElementById("sub").textContent =
    `Cada metrópoli se mide por cuánto crecería su mercado de papa congelada al ${C.anio}. ` +
    `Un modelo XGBoost proyecta los restaurantes de comida rápida y a la carta; cada restaurante ` +
    `vale ${fmt1(C.kg_por_restaurante)} kg al año a US$${C.usd_kg} por kg (importaciones ${C.anio_importaciones}, UN Comtrade).` +
    (R ? ` Las vistas con * son un escenario simulado (supuestos editables, no datos de McCain): cuánto le dejaría a McCain cada ciudad y dónde poner un presupuesto.` : "");
}

function kpis() {
  let lista;
  if (vista === "mercado") {
    const top = [...D.metros].sort((a, b) => a.rank_potencial - b.rank_potencial)[0];
    const mejora = C.mae.Persistencia ? (100 * (1 - C.mae.XGBoost / C.mae.Persistencia)) : null;
    lista = [
      [fmtM(C.total_inc), `Mercado nuevo en ${D.metros.length} metrópolis al ${C.anio}`],
      [top ? top.nombre : "—", "Mayor potencial incremental"],
      [C.mae.XGBoost + " pp", `Error medio del modelo (prueba ${C.periodo_prueba})`],
      [mejora == null ? "—" : (mejora >= 0 ? "−" : "+") + Math.abs(mejora).toFixed(0) + "%", "Error del modelo frente a repetir el crecimiento anterior"]];
  } else if (vista === "rentabilidad") {
    const up = suma(D.metros.map(m => m.util_proy)), uh = suma(D.metros.map(m => m.util_hoy)), vp = suma(D.metros.map(m => m.ventas_proy));
    const top = [...D.metros].sort((a, b) => a.rank_util - b.rank_util)[0];
    lista = [
      [fmtM(up), `Margen de contribución McCain ${C.anio} (hoy ${fmtM(uh)})`],
      [fmtM(vp), `Ventas McCain ${C.anio}`],
      [vp > 0 ? fmtP(100 * up / vp) : "—", "Margen de contribución sobre ventas"],
      [top ? top.nombre : "—", "Ciudad que más deja"]];
  } else {
    const X = S.presupuesto, extra = suma(D.metros.map(m => m.extra || 0)), con = D.metros.filter(m => (m.inv || 0) > 1).length;
    lista = [
      [fmtD(extra), `Contribución extra al año con ${fmtM(X)}`],
      [extra > 0 ? fmt1(X / extra) + " años" : "—", "Para recuperar la inversión"],
      [extra > 0 && X > 0 ? fmtP(100 * extra / X) : "—", "Retorno anual"],
      [String(con), "Ciudades con inversión"]];
  }
  document.getElementById("kpis").innerHTML = lista.map(([b, s]) => `<div class="kpi"><b>${b}</b><span>${s}</span></div>`).join("");
}

function nota() {
  let texto = `Potencial = restaurantes objetivo × kg importados por restaurante × US$/kg; supone consumo y precio constantes. ` +
    `Validación temporal: entrenado con 2015→2019 y 2019→2022, probado con ${C.periodo_prueba}. ` +
    `Spearman entre metrópolis (prueba): XGBoost ${C.spearman.XGBoost ?? "—"}, persistencia ${C.spearman.Persistencia ?? "—"}. Contorno: Natural Earth.`;
  if (R && vista !== "mercado") {
    texto += ` * Escenario simulado, no datos de McCain. Locales por segmento proyectados con XGBoost ` +
      `(${R.ganador === "compartido" ? "un modelo compartido" : "un modelo por segmento"}, el que menos se equivocó en la prueba). ` +
      `Margen de contribución = ventas menos costos variables por kilo (producto, importación y servicio); no incluye costos fijos, inversión ni impuestos. El flete lo paga el cliente.`;
  }
  if (vista === "inversion") {
    texto += ` Reparto: cada dólar va a la ciudad donde deja más contribución extra, partiendo de la participación actual de cada ciudad y pagando una entrada que crece con el tamaño de su mercado; las cadenas no cuentan (se negocian como cuenta nacional). ` +
      `La recuperación supone que la contribución extra llega completa desde el primer año y no descuenta el dinero en el tiempo.`;
  }
  document.getElementById("nota").textContent = texto;
}

// ---------------------------------------------------------------------------
// Paneles de supuestos e inversion
// ---------------------------------------------------------------------------
function campo(id, valor, paso, sufijo, titulo) {
  return `<label class="campo" title="${titulo || ""}"><span>${sufijo === "%" ? "" : sufijo}</span><input data-s="${id}" type="number" step="${paso}" value="${valor}">${sufijo === "%" ? "<span>%</span>" : ""}</label>`;
}

function panelSupuestos() {
  const caja = document.getElementById("supuestos");
  if (!R) return;
  const pc = v => +(100 * v).toFixed(2);
  caja.innerHTML = `<h2>Supuestos del escenario* <button class="boton" id="restablecer" type="button">Restablecer</button></h2>
    <div class="generales">
      <div><span class="tipo">Precio de lista (US$/kg)</span>${campo("precio_lista", S.precio_lista, 0.05, "US$")}</div>
      <div><span class="tipo">Costo del producto (US$/kg, Comtrade ${R.anio_importaciones})</span>${campo("costo_producto", S.costo_producto.toFixed(2), 0.05, "US$", "Precio real de importación; cámbialo para simular otro precio")}</div>
      <div><span class="tipo">Costo de importación (US$/kg)</span>${campo("costo_importacion", S.costo_importacion, 0.05, "US$")}</div>
      <div><span class="tipo">Participación promedio de McCain (resultado)</span><b id="partProm" class="cifra"></b></div>
    </div>
    <div class="desliza"><table class="matriz"><thead><tr><th class="izq">Segmento</th><th>Consumo relativo</th><th>Descuento</th><th>Costo de servir (US$/kg)</th><th>Participación McCain</th><th>kg por local al año</th><th>Precio neto</th><th>Margen por kg</th><th>Margen</th></tr></thead><tbody>
    ${S.segmentos.map((s, i) => `<tr><td class="izq">${s.nombre}</td>
      <td>${campo("seg." + i + ".consumo_relativo", s.consumo_relativo, 0.1, "×")}</td>
      <td>${campo("seg." + i + ".descuento", pc(s.descuento), 1, "%")}</td>
      <td>${campo("seg." + i + ".costo_servir", s.costo_servir, 0.05, "US$")}</td>
      <td>${campo("seg." + i + ".participacion", pc(s.participacion), 1, "%")}</td>
      <td data-c="kg${i}"></td><td data-c="neto${i}"></td><td data-c="mkg${i}"></td><td data-c="mpct${i}"></td></tr>`).join("")}
    </tbody></table></div>
    <p class="tipo">Consumo relativo: kg que compra un local del segmento respecto a un independiente de comida rápida (1.0×). El consumo por local se calibra para que el país sume los kilos que importa México. La participación de McCain cambia por segmento, así que cada ciudad tiene su propia penetración según su mezcla; en la vista Inversión puedes fijar la de una ciudad si conoces la real.</p>`;
  caja.querySelectorAll("input[data-s]").forEach(el => el.addEventListener("input", () => leerCampo(el)));
  document.getElementById("restablecer").addEventListener("click", () => { S = JSON.parse(ORIGINAL); S.part_ciudad = S.part_ciudad || {}; panelSupuestos(); panelInversion(); refrescar(); });
  matrizCalculada();
}

function matrizCalculada() {
  const tot = suma(S.segmentos.map(s => s.locales_pais * (s.kg_local || 0)));
  const prom = tot > 0 ? suma(S.segmentos.map(s => s.locales_pais * (s.kg_local || 0) * s.participacion)) / tot : 0;
  const el = document.getElementById("partProm"); if (el) el.textContent = fmtP(100 * prom);
  S.segmentos.forEach((s, i) => {
    const pon = (k, v) => { const el = document.querySelector(`#supuestos [data-c="${k}${i}"]`); if (el) el.textContent = v; };
    pon("kg", fmtN(s.kg_local)); pon("neto", "US$" + (s.neto || 0).toFixed(2));
    pon("mkg", "US$" + (s.margen_kg || 0).toFixed(2)); pon("mpct", fmtP(100 * (s.margen_pct || 0)));
  });
}

function leerCampo(el) {
  const v = Number(el.value);
  if (el.value === "" || isNaN(v)) return;
  const partes = el.dataset.s.split(".");
  const porcentaje = ["participacion", "descuento", "techo_existente", "techo_nuevo", "entrada_pct"].includes(partes[partes.length - 1]);
  const ultimo = partes[partes.length - 1];
  const valor = porcentaje ? v / 100 : ultimo === "presupuesto" ? v * 1e6 : ultimo === "entrada_fija" ? v * 1e3 : v;
  if (partes[0] === "seg") S.segmentos[+partes[1]][partes[2]] = valor; else S[partes[0]] = valor;
  refrescar();
}

function panelInversion() {
  const caja = document.getElementById("inversion");
  if (!tieneInversion()) return;
  const pc = v => +(100 * v).toFixed(2);
  caja.innerHTML = `<h2>Si McCain tuviera X millones, ¿dónde los pone?*</h2>
    <div class="generales">
      <div><span class="tipo">Presupuesto (US$ millones)</span><label class="campo grande"><span>US$</span><input data-s="presupuesto" type="number" min="0" step="1" value="${S.presupuesto / 1e6}"><span>M</span></label></div>
      <div><span class="tipo">Techo de participación en clientes existentes</span>${campo("techo_existente", pc(S.techo_existente), 1, "%")}</div>
      <div><span class="tipo">Techo de participación en locales nuevos</span>${campo("techo_nuevo", pc(S.techo_nuevo), 1, "%")}</div>
      <div><span class="tipo">Inversión para ganar la mitad del hueco (US$ por kg anual)</span>${campo("costo_mitad", S.costo_mitad, 0.01, "US$")}</div>
      <div><span class="tipo">Entrada: monto fijo por ciudad (US$ miles)</span><label class="campo"><span>US$</span><input data-s="entrada_fija" type="number" min="0" step="10" value="${(S.entrada_fija || 0) / 1e3}"><span>mil</span></label></div>
      <div><span class="tipo">Entrada: % del mercado anual de la ciudad</span>${campo("entrada_pct", pc(S.entrada_pct || 0), 0.5, "%")}</div>
    </div>
    <p class="tipo">Cada ciudad parte de su participación actual: lo que se puede ganar es el hueco hasta el techo. Con la inversión "mitad" se gana 50% del hueco; con el doble, 75%; con el triple, 87.5%. Entrar a una ciudad cuesta un fijo más un % de su mercado anual (barrera de entrada): una ciudad grande pide más para empezar, así que solo se entra donde lo que deja paga esa entrada. Fija la participación real de una ciudad en la tabla de abajo.</p>
    <h2>Curva: lo que rinde invertir más</h2>
    <div class="desliza"><table class="matriz"><thead><tr><th class="izq">Presupuesto</th><th>Contribución extra al año</th><th>Años para recuperar</th><th>Retorno anual</th><th>Retorno del tramo</th><th>Ciudades</th></tr></thead><tbody id="curva"></tbody></table></div>`;
  caja.querySelectorAll("input[data-s]").forEach(el => el.addEventListener("input", () => leerCampo(el)));
}

function curva() {
  const cuerpo = document.getElementById("curva");
  if (!cuerpo) return;
  let previo = null;
  cuerpo.innerHTML = (S.curva || []).map(X => {
    const r = asignar(D.metros, X), extra = suma([...r.values()].map(v => v.extra));
    const tramo = previo ? (extra - previo.extra) / (X - previo.X) : extra / X;
    previo = {X, extra};
    const ciudades = [...r.values()].filter(v => v.inv > 1).length;
    return `<tr><td class="izq">${fmtM(X)}</td><td>${fmtD(extra)}</td><td>${extra > 0 ? fmt1(X / extra) : "—"}</td><td>${fmtP(100 * extra / X)}</td><td>${fmtP(100 * tramo)}</td><td>${ciudades}</td></tr>`;
  }).join("");
}

// ---------------------------------------------------------------------------
// Mapa, tabla y detalle
// ---------------------------------------------------------------------------
function filtrados() {
  const q = document.getElementById("buscar").value.trim().toLowerCase();
  return D.metros.filter(m => !q || m.nombre.toLowerCase().includes(q));
}

function dibujarMapa() {
  const svg = document.getElementById("mapa");
  const metrica = document.getElementById("metrica").value;
  const pts = D.metros.filter(m => m.lat != null && m.lon != null);
  const W = 640, H = 420, pad = 22;
  const lon0 = -118, lon1 = -86.3, lat0 = 14.3, lat1 = 33;
  const x = lon => pad + (lon - lon0) / (lon1 - lon0) * (W - 2 * pad);
  const y = lat => H - pad - (lat - lat0) / (lat1 - lat0) * (H - 2 * pad);
  const maxPot = Math.max(...pts.map(m => m.pot_proy || 0), 1);
  const r = m => 3 + 22 * Math.sqrt((m.pot_proy || 0) / maxPot);
  const visibles = new Set(filtrados().map(m => m.clave));
  let g = "";
  if (D.contorno && D.contorno.length) {
    g += `<path d="M${D.contorno.map(([lo, la]) => x(lo).toFixed(1) + "," + y(la).toFixed(1)).join("L")}Z" fill="var(--tierra)" stroke="var(--line)" stroke-width="1"/>`;
  }
  const ordenados = [...pts].sort((a, b) => (b.pot_proy || 0) - (a.pot_proy || 0));
  const p = pct[metrica] || (() => null);
  for (const m of ordenados) {
    const t = p(m[metrica]); const op = visibles.has(m.clave) ? 0.92 : 0.15;
    g += `<circle class="punto${m.clave === seleccion ? " sel" : ""}" data-clave="${m.clave}" cx="${x(m.lon).toFixed(1)}" cy="${y(m.lat).toFixed(1)}" r="${r(m).toFixed(1)}" fill="${t == null ? "var(--muted)" : rampa(t)}" fill-opacity="${op}"></circle>`;
  }
  // Etiquetas: la seleccionada y las primeras de la vista que no esten pegadas a otra.
  const puestas = [], centros = [];
  const choca = a => puestas.some(b => a.x0 < b.x1 && a.x1 > b.x0 && a.y0 < b.y1 && a.y1 > b.y0);
  const sel = D.metros.find(m => m.clave === seleccion);
  const rk = VISTAS[vista].rank;
  const primeras = [...D.metros].sort((a, b) => (a[rk] || 999) - (b[rk] || 999)).slice(0, 8);
  const etiquetadas = [];
  for (const m of [sel, ...primeras].filter(m => m && m.lat != null)) {
    if (etiquetadas.includes(m)) continue;
    const cx = x(m.lon), cy = y(m.lat);
    if (m !== sel && centros.some(([a, b]) => Math.hypot(a - cx, b - cy) < 45)) continue;
    centros.push([cx, cy]); etiquetadas.push(m);
    if (etiquetadas.length > 5) break;
  }
  etiquetadas.forEach(m => {
    const cx = x(m.lon), cy = y(m.lat), rr = r(m), ancho = m.nombre.length * 6.4;
    const opciones = [[cx + rr + 3, cy + 4, "start"], [cx - rr - 3, cy + 4, "end"], [cx, cy - rr - 4, "middle"], [cx, cy + rr + 12, "middle"]];
    for (const [tx, ty, ancla] of opciones) {
      const x0 = ancla === "start" ? tx : ancla === "end" ? tx - ancho : tx - ancho / 2;
      const caja = {x0, x1: x0 + ancho, y0: ty - 10, y1: ty + 2};
      if (!choca(caja) || ancla === "middle" && ty > cy) {
        puestas.push(caja);
        g += `<text class="etiqueta" x="${tx.toFixed(1)}" y="${ty.toFixed(1)}" text-anchor="${ancla}">${m.nombre}</text>`;
        break;
      }
    }
  });
  svg.innerHTML = g;
  const vals = D.metros.map(m => m[metrica]).filter(v => v != null && !isNaN(v));
  const col = columnas().find(c => c.k === metrica);
  const f = col && col.f ? col.f : fmt1;
  document.getElementById("leyMin").textContent = vals.length ? f(Math.min(...vals)) : "—";
  document.getElementById("leyMax").textContent = vals.length ? f(Math.max(...vals)) : "—";
  svg.querySelectorAll(".punto").forEach(el => {
    el.addEventListener("click", () => seleccionar(el.dataset.clave));
    el.addEventListener("mousemove", ev => tip(ev, D.metros.find(m => m.clave === el.dataset.clave)));
    el.addEventListener("mouseleave", () => document.getElementById("tip").style.display = "none");
  });
}

function tip(ev, m) {
  const t = document.getElementById("tip");
  const linea = vista === "rentabilidad" ? `Contribución ${C.anio}: ${fmtD(m.util_proy)} · margen ${fmtP(m.margen)}`
    : vista === "inversion" ? `Inversión ${fmtD(m.inv)} → +${fmtD(m.extra)} al año` : `+${fmtM(m.pot_inc)} al ${C.anio} · ${fmtP(m.crec_esp)} anual`;
  t.innerHTML = `<b>${m.nombre}</b><br>${linea}`;
  t.style.display = "block";
  t.style.left = Math.min(ev.clientX + 12, window.innerWidth - 260) + "px";
  t.style.top = (ev.clientY + 12) + "px";
}

function dibujarTabla() {
  const cols = columnas();
  const thead = document.querySelector("#heat thead"), tbody = document.querySelector("#heat tbody");
  thead.innerHTML = "<tr>" + cols.map(c => `<th data-k="${c.k}" class="${c.k === "nombre" ? "izq" : ""}">${c.t}${orden.k === c.k ? (orden.asc ? " ▲" : " ▼") : ""}</th>`).join("") + "</tr>";
  const n = +document.getElementById("top").value;
  const filas = filtrados().sort((a, b) => {
    const va = a[orden.k], vb = b[orden.k];
    if (va == null || isNaN(va) && typeof va === "number") return 1; if (vb == null || isNaN(vb) && typeof vb === "number") return -1;
    return (va < vb ? -1 : va > vb ? 1 : 0) * (orden.asc ? 1 : -1);
  }).slice(0, n);
  tbody.innerHTML = filas.map(m => `<tr data-clave="${m.clave}" class="${m.clave === seleccion ? "sel" : ""}">` +
    cols.map(c => {
      const f = c.f || (v => v);
      if (c.editable) return `<td class="editable${m.part_fija ? " fija" : ""}"><label class="campo"><input data-ciudad="${m.clave}" type="number" step="1" min="0" max="100" value="${(+m[c.k]).toFixed(1)}"><span>%</span></label></td>`;
      if (!c.heat) return `<td class="${c.k === "nombre" ? "nombre" : ""}">${f(m[c.k])}</td>`;
      const t = pct[c.k] ? pct[c.k](m[c.k]) : null;
      return `<td style="background:${t == null ? "transparent" : rampa(t)};color:${t == null ? "var(--muted)" : tinta(t)}">${f(m[c.k])}</td>`;
    }).join("") + "</tr>").join("");
  tbody.querySelectorAll("tr").forEach(tr => tr.addEventListener("click", ev => {
    if (ev.target.tagName !== "INPUT") seleccionar(tr.dataset.clave);
  }));
  tbody.querySelectorAll("input[data-ciudad]").forEach(el => el.addEventListener("change", () => {
    const v = Number(el.value);
    if (el.value === "" || isNaN(v)) { delete S.part_ciudad[el.dataset.ciudad]; }
    else { S.part_ciudad[el.dataset.ciudad] = Math.min(Math.max(v, 0), 100) / 100; }
    refrescar();
  }));
  thead.querySelectorAll("th").forEach(th => th.addEventListener("click", () => {
    const k = th.dataset.k;
    orden = {k, asc: orden.k === k ? !orden.asc : (k.startsWith("rank") || k === "nombre")};
    dibujarTabla();
  }));
}

function dibujarDetalle() {
  const m = D.metros.find(x => x.clave === seleccion);
  const caja = document.getElementById("detalle");
  if (!m) { caja.innerHTML = "<p>Haz clic en una metrópoli.</p>"; return; }
  const total = D.metros.length;
  let rentab = "";
  if (R) {
    const principal = m.segmentos && m.segmentos.length ? m.segmentos[0].nombre.toLowerCase() : "—";
    rentab = `<h3>Escenario simulado*</h3><dl>
      <dt>Contribución hoy → ${C.anio}</dt><dd><b>${fmtD(m.util_hoy)} → ${fmtD(m.util_proy)}</b></dd>
      <dt>Ventas McCain ${C.anio}</dt><dd>${fmtD(m.ventas_proy)} · margen ${fmtP(m.margen)}</dd>
      <dt>Participación de McCain hoy</dt><dd>${fmtP(m.part_actual)}${m.part_fija ? " (fijada)" : " (por su mezcla)"}</dd>
      <dt>Lugar por contribución</dt><dd>${m.rank_util} de ${total}</dd>
    </dl>
    ${tieneInversion() ? `<p class="frase">Con un presupuesto de ${fmtM(S.presupuesto)}: ${(m.inv || 0) > 1 ? `recibe <b>${fmtD(m.inv)}</b> (incluye ${fmtD(m.entrada)} de entrada), deja +${fmtD(m.extra)} al año y se recupera en ${fmtA(m.anios)} años.` : "no recibe inversión."}</p>` : ""}
    <table class="seg"><thead><tr><th class="izq">Segmento</th><th>Locales hoy → ${C.anio}</th><th>Contribución ${C.anio}</th></tr></thead><tbody>` +
      m.segmentos.map(s => `<tr><td class="izq">${s.nombre}</td><td>${fmtN(s.hoy)} → ${fmtN(s.proy)}</td><td>${fmtD(s.util)}</td></tr>`).join("") +
      `</tbody></table><p class="tipo">El segmento que más deja: ${principal}.</p>`;
  }
  caja.innerHTML = `<p class="nombre">${m.nombre}</p><p class="tipo">${m.tipo} · puesto ${m.rank_potencial} de ${total} en potencial</p>
    <dl>
      <dt>Potencial incremental al ${C.anio}</dt><dd><b>+${fmtM(m.pot_inc)}</b></dd>
      <dt>Mercado hoy → ${C.anio}</dt><dd>${fmtM(m.pot_hoy)} → ${fmtM(m.pot_proy)}</dd>
      <dt>Restaurantes objetivo</dt><dd>${fmtN(m.rest_hoy)} → ${fmtN(m.rest_proy)}</dd>
      <dt>Crecimiento esperado</dt><dd>${fmtP(m.crec_esp)} anual</dd>
      <dt>Crecimiento 2019–2026</dt><dd>${fmtP(m.crec_hist)} anual</dd>
      <dt>Índice de inversión</dt><dd>${fmt1(m.indice)}${m.rank_indice ? " (#" + m.rank_indice + ")" : ""}</dd>
      <dt>Población 2020</dt><dd>${fmtN(m.poblacion)}</dd>
    </dl>
    <ul>${m.insights.map(s => `<li>${s}</li>`).join("")}</ul>${rentab}`;
}

function seleccionar(clave) { seleccion = clave; dibujarMapa(); dibujarTabla(); dibujarDetalle(); }
function refrescar() { recalcular(); percentiles(); matrizCalculadaSegura(); kpis(); nota(); curva(); dibujarMapa(); dibujarTabla(); dibujarDetalle(); }
function matrizCalculadaSegura() { if (R && document.querySelector("#supuestos table")) matrizCalculada(); }

document.getElementById("buscar").addEventListener("input", () => { dibujarMapa(); dibujarTabla(); });
document.getElementById("metrica").addEventListener("change", dibujarMapa);
document.getElementById("top").addEventListener("change", dibujarTabla);
document.getElementById("tema").addEventListener("click", () => {
  const raiz = document.documentElement;
  const oscuro = raiz.dataset.theme ? raiz.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  raiz.dataset.theme = oscuro ? "light" : "dark"; dibujarMapa(); dibujarTabla();
});

recalcular(); encabezado(); panelSupuestos(); panelInversion(); cambiarVista("mercado");
</script>
</body>
</html>
"""
