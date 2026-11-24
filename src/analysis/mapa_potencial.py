"""
Heatmap interactivo del potencial de mercado por metropoli.

Generamos un solo archivo HTML, sin dependencias externas: los datos van
dentro del archivo, asi se abre en cualquier navegador y en Databricks con
displayHTML. Tiene tres piezas:

    mapa       un punto por metropoli en su latitud/longitud; tamano = mercado
               proyectado, color = la metrica que elijas
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
    datos = json.dumps({"metros": preparar_datos(tabla, mercado), "contexto": contexto},
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
  --r0:#eef4fc; --r1:#1a4f94;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    --bg:#111214; --surface:#1a1b1e; --ink:#f2f2f0; --ink2:#b3b2ad; --muted:#85847f;
    --grid:#2a2b2f; --line:#34353a; --accent:#5b9be6; --sel:#f08a5d;
    --r0:#1c2633; --r1:#7fb3f0;
  }
}
:root[data-theme="dark"]{
  --bg:#111214; --surface:#1a1b1e; --ink:#f2f2f0; --ink2:#b3b2ad; --muted:#85847f;
  --grid:#2a2b2f; --line:#34353a; --accent:#5b9be6; --sel:#f08a5d;
  --r0:#1c2633; --r1:#7fb3f0;
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
      <option value="20">Top 20</option><option value="40">Top 40</option><option value="999">Todas</option>
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
  const W = 640, H = 420, pad = 28;
  const lon0 = -118, lon1 = -86, lat0 = 14, lat1 = 33;
  const x = lon => pad + (lon - lon0) / (lon1 - lon0) * (W - 2 * pad);
  const y = lat => H - pad - (lat - lat0) / (lat1 - lat0) * (H - 2 * pad);
  const maxPot = Math.max(...pts.map(m => m.pot_proy || 0), 1);
  const r = m => 3 + 22 * Math.sqrt((m.pot_proy || 0) / maxPot);
  const visibles = new Set(filtrados().map(m => m.clave));
  let g = "";
  for (let lat = 15; lat <= 32; lat += 5) g += `<line x1="${pad}" x2="${W - pad}" y1="${y(lat)}" y2="${y(lat)}" stroke="var(--grid)"/><text x="${pad - 4}" y="${y(lat) + 4}" text-anchor="end">${lat}°</text>`;
  for (let lon = -115; lon <= -90; lon += 5) g += `<line y1="${pad}" y2="${H - pad}" x1="${x(lon)}" x2="${x(lon)}" stroke="var(--grid)"/><text x="${x(lon)}" y="${H - pad + 16}" text-anchor="middle">${lon}°</text>`;
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
