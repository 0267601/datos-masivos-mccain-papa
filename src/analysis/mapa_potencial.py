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


def generar(tabla, metricas, mercado, importancia, ruta=None):
    """Regresamos el HTML; si damos ruta, ademas lo escribimos."""
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
    datos = json.dumps({"metros": preparar_datos(tabla, mercado), "contexto": contexto,
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
.panel h2{font-size:14px;margin:0 0 8px}
svg text{fill:var(--ink2);font-size:11px}
.punto{cursor:pointer;stroke:var(--surface);stroke-width:1.5}
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
  <div class="kpis" id="kpis"></div>
  <div class="controles">
    <input id="buscar" type="search" placeholder="Buscar metrópoli…" aria-label="Buscar metrópoli">
    <select id="metrica" aria-label="Color del mapa">
      <option value="pot_inc">Color: potencial incremental (US$)</option>
      <option value="crec_esp">Color: crecimiento esperado (%)</option>
      <option value="indice">Color: índice de inversión</option>
    </select>
    <select id="top" aria-label="Metrópolis en la tabla">
      <option value="999" selected>Todas</option><option value="20">Top 20</option><option value="40">Top 40</option>
    </select>
    <button class="boton" id="tema" type="button">Tema</button>
  </div>
  <div class="grid">
    <div class="panel">
      <h2>Mapa: tamaño = mercado proyectado, color = métrica elegida</h2>
      <svg id="mapa" viewBox="0 0 640 420" role="img" aria-label="Mapa de metrópolis por potencial"></svg>
      <div class="leyenda"><span id="leyMin"></span><div class="rampa"></div><span id="leyMax"></span></div>
    </div>
    <div class="panel" id="detalle" aria-live="polite"></div>
  </div>
  <div class="panel tabla">
    <h2>Heatmap: cada celda coloreada por su percentil dentro de la columna</h2>
    <table id="heat"><thead></thead><tbody></tbody></table>
  </div>
  <p class="nota" id="nota"></p>
</div>
<div id="tip"></div>
<script>
const D = /*__DATOS__*/null;
const C = D.contexto;
const fmtM = v => v == null ? "—" : "US$" + (v / 1e6).toLocaleString("es-MX", {minimumFractionDigits: 1, maximumFractionDigits: 1}) + " M";
const fmtN = v => v == null ? "—" : Math.round(v).toLocaleString("es-MX");
const fmtP = v => v == null ? "—" : v.toLocaleString("es-MX", {minimumFractionDigits: 1, maximumFractionDigits: 1}) + "%";
const fmt1 = v => v == null ? "—" : v.toLocaleString("es-MX", {minimumFractionDigits: 1, maximumFractionDigits: 1});

const COLUMNAS = [
  {k: "rank_potencial", t: "#", f: v => v, heat: false},
  {k: "nombre", t: "Metrópoli", f: v => v, heat: false},
  {k: "pot_inc", t: "Potencial incremental", f: fmtM, heat: true},
  {k: "pot_proy", t: "Mercado " + C.anio, f: fmtM, heat: true},
  {k: "crec_esp", t: "Crec. esperado", f: fmtP, heat: true},
  {k: "crec_hist", t: "Crec. 2019–26", f: fmtP, heat: true},
  {k: "rest_hoy", t: "Restaurantes hoy", f: fmtN, heat: true},
  {k: "cadenas_100k", t: "Cadenas netas/100k", f: fmt1, heat: true},
  {k: "hoteles_100k", t: "Hoteles/100k", f: fmt1, heat: true},
  {k: "indice", t: "Índice", f: fmt1, heat: true},
];

// Percentil de cada valor dentro de su columna (0 a 1).
const pct = {};
COLUMNAS.filter(c => c.heat).forEach(c => {
  const vals = D.metros.map(m => m[c.k]).filter(v => v != null).sort((a, b) => a - b);
  pct[c.k] = v => {
    if (v == null || vals.length < 2) return null;
    let i = 0; while (i < vals.length && vals[i] < v) i++;
    return i / (vals.length - 1);
  };
});

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

let seleccion = D.metros[0] ? D.metros[0].clave : null;
let orden = {k: "rank_potencial", asc: true};

function encabezado() {
  document.getElementById("sub").textContent =
    `Cada metrópoli se mide por cuánto crecería su mercado de papa congelada al ${C.anio}. ` +
    `Un modelo XGBoost proyecta los restaurantes de comida rápida y a la carta; cada restaurante ` +
    `vale ${fmt1(C.kg_por_restaurante)} kg al año a US$${C.usd_kg} por kg (importaciones ${C.anio_importaciones}, UN Comtrade). ` +
    `Es tamaño de mercado, no utilidad.`;
  const top = D.metros[0];
  const mejora = C.mae.Persistencia ? (100 * (1 - C.mae.XGBoost / C.mae.Persistencia)) : null;
  document.getElementById("kpis").innerHTML = [
    [fmtM(C.total_inc), `Mercado nuevo en ${D.metros.length} metrópolis al ${C.anio}`],
    [top ? top.nombre : "—", "Mayor potencial incremental"],
    [C.mae.XGBoost + " pp", `Error medio del modelo (prueba ${C.periodo_prueba})`],
    [mejora == null ? "—" : (mejora >= 0 ? "−" : "+") + Math.abs(mejora).toFixed(0) + "%",
     "Error del modelo frente a repetir el crecimiento anterior"],
  ].map(([b, s]) => `<div class="kpi"><b>${b}</b><span>${s}</span></div>`).join("");
  document.getElementById("nota").textContent =
    `Potencial = restaurantes objetivo × kg importados por restaurante × US$/kg. ` +
    `Supone que el consumo por restaurante y el precio se mantienen; no incluye márgenes, costos ni papa nacional. ` +
    `Validación temporal: entrenado con 2015→2019 y 2019→2022, probado con ${C.periodo_prueba}. ` +
    `Contorno: Natural Earth. ` +
    `Spearman entre metrópolis (prueba): XGBoost ${C.spearman.XGBoost ?? "—"}, persistencia ${C.spearman.Persistencia ?? "—"}.`;
}

function filtrados() {
  const q = document.getElementById("buscar").value.trim().toLowerCase();
  return D.metros.filter(m => !q || m.nombre.toLowerCase().includes(q));
}

function dibujarMapa() {
  const svg = document.getElementById("mapa");
  const metrica = document.getElementById("metrica").value;
  const pts = D.metros.filter(m => m.lat != null && m.lon != null);
  const W = 640, H = 420, pad = 14;
  const lon0 = -117.5, lon1 = -86.5, lat0 = 14.3, lat1 = 33;
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
  const p = pct[metrica];
  for (const m of ordenados) {
    const t = p(m[metrica]); const op = visibles.has(m.clave) ? 0.92 : 0.15;
    g += `<circle class="punto${m.clave === seleccion ? " sel" : ""}" data-clave="${m.clave}" cx="${x(m.lon).toFixed(1)}" cy="${y(m.lat).toFixed(1)}" r="${r(m).toFixed(1)}" fill="${t == null ? "var(--muted)" : rampa(t)}" fill-opacity="${op}"></circle>`;
  }
  // Etiquetas solo para las 6 de mayor potencial incremental; probamos a la
  // derecha, a la izquierda, arriba y abajo hasta no encimar otra etiqueta.
  const puestas = [];
  const choca = (a) => puestas.some(b => a.x0 < b.x1 && a.x1 > b.x0 && a.y0 < b.y1 && a.y1 > b.y0);
  D.metros.slice(0, 6).forEach(m => {
    if (m.lat == null) return;
    const cx = x(m.lon), cy = y(m.lat), rr = r(m), ancho = m.nombre.length * 6.4;
    const opciones = [
      [cx + rr + 3, cy + 4, "start"], [cx - rr - 3, cy + 4, "end"],
      [cx, cy - rr - 4, "middle"], [cx, cy + rr + 12, "middle"]];
    for (const [tx, ty, ancla] of opciones) {
      const x0 = ancla === "start" ? tx : ancla === "end" ? tx - ancho : tx - ancho / 2;
      const caja = {x0, x1: x0 + ancho, y0: ty - 10, y1: ty + 2};
      if (!choca(caja) || ancla === "middle" && ty > cy) {
        puestas.push(caja);
        g += `<text x="${tx.toFixed(1)}" y="${ty.toFixed(1)}" text-anchor="${ancla}" style="fill:var(--ink);font-weight:600">${m.nombre}</text>`;
        break;
      }
    }
  });
  svg.innerHTML = g;
  const vals = D.metros.map(m => m[metrica]).filter(v => v != null);
  const f = COLUMNAS.find(c => c.k === metrica).f;
  document.getElementById("leyMin").textContent = f(Math.min(...vals));
  document.getElementById("leyMax").textContent = f(Math.max(...vals));
  svg.querySelectorAll(".punto").forEach(el => {
    el.addEventListener("click", () => seleccionar(el.dataset.clave));
    el.addEventListener("mousemove", ev => tip(ev, D.metros.find(m => m.clave === el.dataset.clave)));
    el.addEventListener("mouseleave", () => document.getElementById("tip").style.display = "none");
  });
}

function tip(ev, m) {
  const t = document.getElementById("tip");
  t.innerHTML = `<b>${m.nombre}</b><br>+${fmtM(m.pot_inc)} al ${C.anio} · ${fmtP(m.crec_esp)} anual`;
  t.style.display = "block";
  t.style.left = Math.min(ev.clientX + 12, window.innerWidth - 230) + "px";
  t.style.top = (ev.clientY + 12) + "px";
}

function dibujarTabla() {
  const thead = document.querySelector("#heat thead"), tbody = document.querySelector("#heat tbody");
  thead.innerHTML = "<tr>" + COLUMNAS.map(c => `<th data-k="${c.k}" class="${c.k === "nombre" ? "izq" : ""}">${c.t}${orden.k === c.k ? (orden.asc ? " ▲" : " ▼") : ""}</th>`).join("") + "</tr>";
  const n = +document.getElementById("top").value;
  const filas = filtrados().sort((a, b) => {
    const va = a[orden.k], vb = b[orden.k];
    if (va == null) return 1; if (vb == null) return -1;
    return (va < vb ? -1 : va > vb ? 1 : 0) * (orden.asc ? 1 : -1);
  }).slice(0, n);
  tbody.innerHTML = filas.map(m => `<tr data-clave="${m.clave}" class="${m.clave === seleccion ? "sel" : ""}">` +
    COLUMNAS.map(c => {
      if (!c.heat) return `<td class="${c.k === "nombre" ? "nombre" : ""}">${c.f(m[c.k])}</td>`;
      const t = pct[c.k](m[c.k]);
      return `<td style="background:${t == null ? "transparent" : rampa(t)};color:${t == null ? "var(--muted)" : tinta(t)}">${c.f(m[c.k])}</td>`;
    }).join("") + "</tr>").join("");
  tbody.querySelectorAll("tr").forEach(tr => tr.addEventListener("click", () => seleccionar(tr.dataset.clave)));
  thead.querySelectorAll("th").forEach(th => th.addEventListener("click", () => {
    const k = th.dataset.k;
    orden = {k, asc: orden.k === k ? !orden.asc : (k === "rank_potencial" || k === "nombre")};
    dibujarTabla();
  }));
}

function dibujarDetalle() {
  const m = D.metros.find(x => x.clave === seleccion);
  const caja = document.getElementById("detalle");
  if (!m) { caja.innerHTML = "<p>Haz clic en una metrópoli.</p>"; return; }
  caja.innerHTML = `<p class="nombre">${m.nombre}</p><p class="tipo">${m.tipo} · puesto ${m.rank_potencial} de ${D.metros.length}</p>
    <dl>
      <dt>Potencial incremental al ${C.anio}</dt><dd><b>+${fmtM(m.pot_inc)}</b></dd>
      <dt>Mercado hoy → ${C.anio}</dt><dd>${fmtM(m.pot_hoy)} → ${fmtM(m.pot_proy)}</dd>
      <dt>Restaurantes objetivo</dt><dd>${fmtN(m.rest_hoy)} → ${fmtN(m.rest_proy)}</dd>
      <dt>Crecimiento esperado</dt><dd>${fmtP(m.crec_esp)} anual</dd>
      <dt>Crecimiento 2019–2026</dt><dd>${fmtP(m.crec_hist)} anual</dd>
      <dt>Índice de inversión</dt><dd>${fmt1(m.indice)}${m.rank_indice ? " (#" + m.rank_indice + ")" : ""}</dd>
      <dt>Población 2020</dt><dd>${fmtN(m.poblacion)}</dd>
    </dl>
    <ul>${m.insights.map(s => `<li>${s}</li>`).join("")}</ul>`;
}

function seleccionar(clave) { seleccion = clave; dibujarMapa(); dibujarTabla(); dibujarDetalle(); }
function todo() { dibujarMapa(); dibujarTabla(); dibujarDetalle(); }

document.getElementById("buscar").addEventListener("input", todo);
document.getElementById("metrica").addEventListener("change", dibujarMapa);
document.getElementById("top").addEventListener("change", dibujarTabla);
document.getElementById("tema").addEventListener("click", () => {
  const raiz = document.documentElement;
  const oscuro = raiz.dataset.theme ? raiz.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  raiz.dataset.theme = oscuro ? "light" : "dark"; todo();
});
encabezado(); todo();
</script>
</body>
</html>
"""
