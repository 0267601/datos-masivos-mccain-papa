"""
Rentabilidad por metropoli y segmento: ESCENARIO SIMULADO.

El modelo dice cuantos locales de cada segmento habra en 2030 (dato real
proyectado). Lo que no tenemos son los numeros de la empresa: precio,
descuentos, costos y participacion. Esos van como supuestos editables en
config/config.example.yaml y en el Excel que genera este modulo. No son
datos de McCain.

    kg por local      consumo relativo del segmento x kg por unidad de consumo
                      (calibrado para que todo el pais sume los kg reales que
                      importa Mexico segun UN Comtrade)
    precio neto       precio de lista x (1 - descuento del segmento)
    margen por kg     precio neto - costo del producto (Comtrade) - costo de
                      importacion - costo de servir del segmento
    utilidad          locales x kg por local x participacion x margen por kg

El flete lo paga el cliente: no entra al costo.

Salidas:
    processed/rentabilidad_segmentos              metropoli x segmento
    processed/rentabilidad_metropolis             una fila por metropoli
    reports/tables/rentabilidad_metropolis.csv
    reports/tables/rentabilidad_segmentos.csv
    reports/tables/modelo_segmentos_metricas.csv  compartido vs por segmento
    reports/rentabilidad_escenario.xlsx           supuestos + formulas vivas
    reports/figures/mapa_potencial.html           con la utilidad del escenario
"""

import json
import logging
import sys
import time

import numpy as np
import pandas as pd

from src.analysis import modelo_segmentos as ms
from src.utils.config import cargar_config
from src.utils.rutas import FIGURAS, PROCESSED, REPORTS, TABLAS, asegurar_carpetas
from src.utils.spark import cerrar_spark, crear_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("rentabilidad")

SALIDAS = {
    "segmentos": PROCESSED / "rentabilidad_segmentos",
    "metropolis": PROCESSED / "rentabilidad_metropolis",
    "tabla_metropolis": TABLAS / "rentabilidad_metropolis.csv",
    "tabla_segmentos": TABLAS / "rentabilidad_segmentos.csv",
    "metricas": TABLAS / "modelo_segmentos_metricas.csv",
    "supuestos": TABLAS / "rentabilidad_supuestos.json",
    "excel": REPORTS / "rentabilidad_escenario.xlsx",
}


# ---------------------------------------------------------------------------
# 1. Supuestos y calibracion
# ---------------------------------------------------------------------------

def matriz_supuestos(cfg, mercado, nacional):
    """Una fila por segmento con todo lo que se necesita para calcular."""
    costo_producto = float(mercado["usd_kg"])
    filas = []
    for clave, s in cfg["segmentos"].items():
        filas.append({"segmento": clave, "nombre": s["nombre"],
                      "locales_pais": int(nacional.get(clave, 0)),
                      "consumo_relativo": float(s["consumo_relativo"]),
                      "descuento": float(s["descuento"]),
                      "costo_servir_usd_kg": float(s["costo_servir_usd_kg"])})
    matriz = pd.DataFrame(filas)
    unidades = (matriz["locales_pais"] * matriz["consumo_relativo"]).sum()
    if unidades <= 0:
        raise ValueError("Rentabilidad: no hay locales para calibrar el consumo")
    kg_unidad = mercado["kg_importados"] / unidades
    matriz["kg_por_local"] = matriz["consumo_relativo"] * kg_unidad
    matriz["precio_neto"] = cfg["precio_lista_usd_kg"] * (1 - matriz["descuento"])
    matriz["margen_usd_kg"] = (matriz["precio_neto"] - costo_producto
                               - cfg["costo_importacion_usd_kg"] - matriz["costo_servir_usd_kg"])
    matriz["margen_pct"] = np.where(matriz["precio_neto"] > 0,
                                    matriz["margen_usd_kg"] / matriz["precio_neto"], 0.0)
    generales = {"precio_lista_usd_kg": float(cfg["precio_lista_usd_kg"]),
                 "costo_producto_usd_kg": costo_producto,
                 "costo_importacion_usd_kg": float(cfg["costo_importacion_usd_kg"]),
                 "participacion_mccain": float(cfg["participacion_mccain"]),
                 "kg_importados": float(mercado["kg_importados"]),
                 "anio_importaciones": int(mercado["anio_importaciones"]),
                 "kg_por_unidad": float(kg_unidad)}
    return matriz, generales


# ---------------------------------------------------------------------------
# 2. Calculo
# ---------------------------------------------------------------------------

def calcular(proyeccion, matriz, generales):
    """Utilidad hoy y proyectada por metropoli y segmento (mismas formulas que el Excel)."""
    datos = proyeccion.merge(matriz, on="segmento", how="left")
    part = generales["participacion_mccain"]
    for momento, locales in [("hoy", "locales_hoy"), ("proy", "locales_proyectados")]:
        kg_mccain = datos[locales] * datos["kg_por_local"] * part
        datos[f"kg_mercado_{momento}"] = datos[locales] * datos["kg_por_local"]
        datos[f"ventas_{momento}_usd"] = kg_mccain * datos["precio_neto"]
        datos[f"utilidad_{momento}_usd"] = kg_mccain * datos["margen_usd_kg"]
    datos["utilidad_incremental_usd"] = datos["utilidad_proy_usd"] - datos["utilidad_hoy_usd"]
    return datos


def resumir(detalle, potencial):
    """Una fila por metropoli, con la utilidad de cada segmento en columnas."""
    suma = (detalle.groupby("clave_metropoli")
            [["locales_hoy", "locales_proyectados", "ventas_hoy_usd", "ventas_proy_usd",
              "utilidad_hoy_usd", "utilidad_proy_usd", "utilidad_incremental_usd"]].sum())
    por_segmento = detalle.pivot_table(index="clave_metropoli", columns="segmento",
                                       values="utilidad_proy_usd", aggfunc="sum")
    por_segmento.columns = [f"utilidad_proy_{c}_usd" for c in por_segmento.columns]
    tabla = suma.join(por_segmento).reset_index()
    tabla["margen_proy_pct"] = np.where(tabla["ventas_proy_usd"] > 0,
                                        tabla["utilidad_proy_usd"] / tabla["ventas_proy_usd"], 0.0)
    principal = (detalle.sort_values("utilidad_proy_usd", ascending=False)
                 .drop_duplicates("clave_metropoli").set_index("clave_metropoli")["nombre"])
    tabla["segmento_principal"] = tabla["clave_metropoli"].map(principal)
    tabla["ranking_utilidad"] = (tabla["utilidad_proy_usd"]
                                 .rank(ascending=False, method="first").astype(int))
    contexto = potencial[["clave_metropoli", "metropoli", "ranking", "ranking_potencial"]]
    return (tabla.merge(contexto, on="clave_metropoli", how="left")
            .sort_values("ranking_utilidad").reset_index(drop=True))


def validar(detalle, resumen, proyeccion):
    """Si los totales no cuadran, no publicamos."""
    if resumen["clave_metropoli"].duplicated().any():
        raise ValueError("Rentabilidad: metropolis repetidas")
    if not np.isclose(detalle["locales_hoy"].sum(), proyeccion["locales_hoy"].sum()):
        raise ValueError("Rentabilidad: los locales de hoy no suman lo del modelo")
    if not np.isclose(resumen["utilidad_proy_usd"].sum(), detalle["utilidad_proy_usd"].sum()):
        raise ValueError("Rentabilidad: el resumen no suma el detalle")
    if detalle["kg_por_local"].isna().any():
        raise ValueError("Rentabilidad: hay segmentos sin supuestos")
    return {"filas": len(resumen), "estado": "VALIDADO"}


# ---------------------------------------------------------------------------
# 3. Excel con formulas vivas
# ---------------------------------------------------------------------------

def escribir_excel(detalle, resumen, matriz, generales, ruta):
    """Supuestos editables y todo lo demas con formulas que dependen de ellos."""
    from openpyxl import Workbook
    from openpyxl.comments import Comment
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    AZUL, VERDE = "0000FF", "008000"
    letra = "Arial"
    titulo = Font(name=letra, bold=True, size=14)
    negrita = Font(name=letra, bold=True)
    normal = Font(name=letra)
    entrada = Font(name=letra, color=AZUL)
    liga = Font(name=letra, color=VERDE)
    gris = Font(name=letra, italic=True, color="595959")
    encabezado_fondo = PatternFill("solid", fgColor="DDEBF7")
    amarillo = PatternFill("solid", fgColor="FFFF00")
    linea = Border(bottom=Side(style="thin", color="999999"))
    USD = '$#,##0;($#,##0);"-"'
    USD2 = '$#,##0.00;($#,##0.00);"-"'
    PCT = '0.0%;(0.0%);"-"'
    NUM = '#,##0;(#,##0);"-"'

    def encabezados(hoja, fila, textos):
        for col, texto in enumerate(textos, start=1):
            celda = hoja.cell(row=fila, column=col, value=texto)
            celda.font, celda.fill, celda.border = negrita, encabezado_fondo, linea
            celda.alignment = Alignment(wrap_text=True, vertical="center")

    def anchos(hoja, valores):
        for col, ancho in enumerate(valores, start=1):
            hoja.column_dimensions[get_column_letter(col)].width = ancho

    wb = Workbook()

    # --- Leeme --------------------------------------------------------------
    hoja = wb.active
    hoja.title = "Leeme"
    lineas = [
        ("Rentabilidad por metrópoli y segmento — ESCENARIO SIMULADO", titulo),
        ("No son datos de McCain. Los precios, descuentos, costos y la participación son supuestos "
         "para mostrar cómo se calcularía el margen de contribución; cámbialos en la hoja Supuestos.", gris),
        ("", normal),
        ("Qué es dato real", negrita),
        ("• Locales de cada segmento hoy (DENUE de INEGI, mayo 2026) y en 2030 (proyección del modelo XGBoost).", normal),
        (f"• Kilos importados y precio por kilo de la papa congelada (UN Comtrade, HS 200410, {generales['anio_importaciones']}).", normal),
        ("", normal),
        ("Qué es supuesto (editable)", negrita),
        ("• Precio de lista, costo de importación, participación de McCain y, por segmento: consumo relativo, "
         "descuento y costo de servir. El flete lo paga el cliente.", normal),
        ("", normal),
        ("Fórmula", negrita),
        ("Margen de contribución = locales × kg por local × participación × (precio neto − costo del producto − "
         "costo de importación − costo de servir)", normal),
        ("kg por local = consumo relativo × kg por unidad de consumo, calibrado para que todo el país "
         "sume los kilos que importa México.", normal),
        ("Es margen de contribución, no utilidad neta: descuenta los costos variables por kilo (producto, "
         "importación y servicio) pero no los costos fijos (fuerza de ventas, oficinas, marketing), la "
         "inversión para entrar a cada ciudad ni los impuestos.", normal),
        ("", normal),
        ("Hojas", negrita),
        ("Supuestos: la matriz editable · Calculo: metrópoli × segmento con fórmulas · "
         "Resumen: una fila por metrópoli · Sensibilidad: contribución total al mover precio, participación y costo.", normal),
        ("", normal),
        ("Colores", negrita),
        ("Azul: supuesto o dato que puedes cambiar · Negro: fórmula · Verde: liga a otra hoja · "
         "Fondo amarillo: supuestos clave.", normal),
    ]
    for i, (texto, fuente) in enumerate(lineas, start=1):
        hoja.cell(row=i, column=1, value=texto).font = fuente
    hoja.column_dimensions["A"].width = 130

    # --- Supuestos ----------------------------------------------------------
    sup = wb.create_sheet("Supuestos")
    sup["A1"] = "Supuestos del escenario"
    sup["A1"].font = titulo
    sup["A2"] = "Celdas azules: edítalas y todo el libro se recalcula."
    sup["A2"].font = gris
    encabezados(sup, 4, ["Supuesto general", "Valor", "Unidad", "Fuente"])
    generales_filas = [
        ("Precio de lista", generales["precio_lista_usd_kg"], "US$/kg", "Supuesto del escenario", USD2, True),
        ("Costo del producto importado", generales["costo_producto_usd_kg"], "US$/kg",
         f"UN Comtrade {generales['anio_importaciones']}, HS 200410: valor ÷ kilos (dato real)", USD2, False),
        ("Costo de importación (aranceles, agente, almacén)", generales["costo_importacion_usd_kg"],
         "US$/kg", "Supuesto del escenario", USD2, True),
        ("Participación de McCain en el mercado", generales["participacion_mccain"], "%",
         "Supuesto del escenario", PCT, True),
        ("Kilos importados en el año", generales["kg_importados"], "kg",
         f"UN Comtrade {generales['anio_importaciones']}, HS 200410 (dato real)", NUM, False),
    ]
    for i, (texto, valor, unidad, fuente, formato, clave) in enumerate(generales_filas, start=5):
        sup.cell(row=i, column=1, value=texto).font = normal
        celda = sup.cell(row=i, column=2, value=valor)
        celda.font, celda.number_format = entrada, formato
        if clave:
            celda.fill = amarillo
        sup.cell(row=i, column=3, value=unidad).font = normal
        sup.cell(row=i, column=4, value=fuente).font = gris
    n = len(matriz)
    f0, f1 = 14, 14 + n - 1
    sup["A10"] = "kg por unidad de consumo (calibrado)"
    sup["A10"].font = normal
    sup["B10"] = f"=IF(SUMPRODUCT(C{f0}:C{f1},D{f0}:D{f1})=0,0,B9/SUMPRODUCT(C{f0}:C{f1},D{f0}:D{f1}))"
    sup["B10"].number_format = '#,##0.0'
    sup["C10"] = "kg"
    sup["D10"] = "Kilos importados ÷ Σ(locales en el país × consumo relativo)"
    sup["D10"].font = gris

    sup["A12"] = "Matriz por segmento"
    sup["A12"].font = negrita
    encabezados(sup, 13, ["Clave", "Segmento", "Locales en el país (2026)", "Consumo relativo",
                          "kg por local al año", "Descuento sobre lista", "Precio neto (US$/kg)",
                          "Costo de servir (US$/kg)", "Margen (US$/kg)", "Margen %"])
    for i, fila in enumerate(matriz.itertuples(index=False), start=f0):
        sup.cell(row=i, column=1, value=fila.segmento).font = normal
        sup.cell(row=i, column=2, value=fila.nombre).font = normal
        c = sup.cell(row=i, column=3, value=fila.locales_pais)
        c.font, c.number_format = entrada, NUM
        c.comment = Comment("Dato real: DENUE mayo 2026, todo el país.", "Pipeline")
        for col, valor, formato in [(4, fila.consumo_relativo, '0.0"x"'), (6, fila.descuento, PCT),
                                    (8, fila.costo_servir_usd_kg, USD2)]:
            c = sup.cell(row=i, column=col, value=valor)
            c.font, c.number_format, c.fill = entrada, formato, amarillo
        sup.cell(row=i, column=5, value=f"=D{i}*$B$10").number_format = NUM
        sup.cell(row=i, column=7, value=f"=$B$5*(1-F{i})").number_format = USD2
        sup.cell(row=i, column=9, value=f"=G{i}-$B$6-$B$7-H{i}").number_format = USD2
        sup.cell(row=i, column=10, value=f"=IF(G{i}=0,0,I{i}/G{i})").number_format = PCT
        for col in (5, 7, 9, 10):
            sup.cell(row=i, column=col).font = normal
    sup.cell(row=f1 + 2, column=1,
             value="Consumo relativo: kg que compra un local del segmento respecto a un independiente "
                   "de comida rápida (1.0x). Las cadenas compran más por local; a la carta, menos.").font = gris
    anchos(sup, [44, 30, 16, 14, 14, 14, 14, 14, 14, 12])
    sup.freeze_panes = "A5"

    # --- Calculo ------------------------------------------------------------
    calc = wb.create_sheet("Calculo")
    columnas = ["Clave metrópoli", "Metrópoli", "Clave segmento", "Segmento",
                "Locales hoy (2026)", "Locales 2030 (modelo)", "Crecimiento esperado anual",
                "kg por local", "kg mercado hoy", "kg mercado 2030", "kg McCain 2030",
                "Precio neto (US$/kg)", "Margen (US$/kg)", "Ventas McCain hoy (US$)",
                "Ventas McCain 2030 (US$)", "Contribución hoy (US$)", "Contribución 2030 (US$)",
                "Contribución incremental (US$)"]
    encabezados(calc, 1, columnas)
    rango_clave = f"Supuestos!$A${f0}:$A${f1}"
    orden = detalle.merge(resumen[["clave_metropoli", "ranking_utilidad"]], on="clave_metropoli")
    orden = orden.sort_values(["ranking_utilidad", "segmento"]).reset_index(drop=True)
    for r, fila in enumerate(orden.itertuples(index=False), start=2):
        valores = [fila.clave_metropoli, fila.metropoli, fila.segmento, fila.nombre,
                   fila.locales_hoy, round(float(fila.locales_proyectados), 2),
                   (fila.crecimiento_esperado / 100) if pd.notna(fila.crecimiento_esperado) else None]
        for col, valor in enumerate(valores, start=1):
            calc.cell(row=r, column=col, value=valor).font = normal
        formulas = [
            f"=INDEX(Supuestos!$E${f0}:$E${f1},MATCH(C{r},{rango_clave},0))",
            f"=E{r}*H{r}", f"=F{r}*H{r}", f"=J{r}*Supuestos!$B$8",
            f"=INDEX(Supuestos!$G${f0}:$G${f1},MATCH(C{r},{rango_clave},0))",
            f"=INDEX(Supuestos!$I${f0}:$I${f1},MATCH(C{r},{rango_clave},0))",
            f"=I{r}*Supuestos!$B$8*L{r}", f"=K{r}*L{r}",
            f"=I{r}*Supuestos!$B$8*M{r}", f"=K{r}*M{r}", f"=Q{r}-P{r}",
        ]
        for col, formula in enumerate(formulas, start=8):
            celda = calc.cell(row=r, column=col, value=formula)
            celda.font = liga if "Supuestos!" in formula else normal
        for col, formato in [(5, NUM), (6, NUM), (7, PCT), (8, NUM), (9, NUM), (10, NUM), (11, NUM),
                             (12, USD2), (13, USD2), (14, USD), (15, USD), (16, USD), (17, USD),
                             (18, USD)]:
            calc.cell(row=r, column=col).number_format = formato
    ultima = len(orden) + 1
    calc.cell(row=ultima + 2, column=1,
              value="Locales hoy y 2030 y el crecimiento esperado vienen del modelo XGBoost por segmento; "
                    "el resto se calcula con los supuestos.").font = gris
    anchos(calc, [12, 26, 18, 28] + [14] * 14)
    calc.freeze_panes = "E2"

    # --- Resumen ------------------------------------------------------------
    res = wb.create_sheet("Resumen")
    res["A1"] = "Margen de contribución por metrópoli — escenario simulado"
    res["A1"].font = titulo
    res["A2"] = "Todo se calcula con fórmulas desde Calculo y Supuestos; el orden es el del escenario base."
    res["A2"].font = gris

    encabezados(res, 4, ["Segmento", "Ventas McCain 2030 (US$)", "Contribución 2030 (US$)",
                         "Margen 2030", "Participación en la contribución"])
    rc = f"Calculo!$C$2:$C${ultima}"
    for i, fila in enumerate(matriz.itertuples(index=False), start=5):
        res.cell(row=i, column=1, value=fila.nombre).font = normal
        res.cell(row=i, column=2, value=f"=SUMIFS(Calculo!$O$2:$O${ultima},{rc},\"{fila.segmento}\")")
        res.cell(row=i, column=3, value=f"=SUMIFS(Calculo!$Q$2:$Q${ultima},{rc},\"{fila.segmento}\")")
        res.cell(row=i, column=4, value=f"=IF(B{i}=0,0,C{i}/B{i})")
        res.cell(row=i, column=5, value=f"=IF($C${5 + n}=0,0,C{i}/$C${5 + n})")
    total = 5 + n
    res.cell(row=total, column=1, value="Total").font = negrita
    res.cell(row=total, column=2, value=f"=SUM(B5:B{total - 1})")
    res.cell(row=total, column=3, value=f"=SUM(C5:C{total - 1})")
    res.cell(row=total, column=4, value=f"=IF(B{total}=0,0,C{total}/B{total})")
    res.cell(row=total, column=5, value=f"=SUM(E5:E{total - 1})")
    for i in range(5, total + 1):
        for col, formato in [(2, USD), (3, USD), (4, PCT), (5, PCT)]:
            res.cell(row=i, column=col).number_format = formato
            res.cell(row=i, column=col).font = negrita if i == total else liga if col < 4 else normal

    inicio = total + 3
    segs = list(matriz.itertuples(index=False))
    cab = (["#", "Metrópoli", "Clave", "Locales hoy", "Locales 2030", "Ventas McCain 2030 (US$)",
            "Contribución hoy (US$)", "Contribución 2030 (US$)", "Contribución incremental (US$)", "Margen 2030"]
           + [f"Contribución 2030: {s.nombre}" for s in segs]
           + ["Lugar en el índice", "Lugar en potencial"])
    encabezados(res, inicio, cab)
    rk = f"Calculo!$A$2:$A${ultima}"
    primera, ultima_res = inicio + 1, inicio + len(resumen)
    for r, fila in enumerate(resumen.itertuples(index=False), start=primera):
        res.cell(row=r, column=1, value=f"=RANK(H{r},$H${primera}:$H${ultima_res})")
        res.cell(row=r, column=2, value=fila.metropoli).font = normal
        res.cell(row=r, column=3, value=fila.clave_metropoli).font = normal
        for col, letra_calc in [(4, "E"), (5, "F"), (6, "O"), (7, "P"), (8, "Q"), (9, "R")]:
            res.cell(row=r, column=col,
                     value=f"=SUMIFS(Calculo!${letra_calc}$2:${letra_calc}${ultima},{rk},C{r})").font = liga
        res.cell(row=r, column=10, value=f"=IF(F{r}=0,0,H{r}/F{r})")
        for j, s in enumerate(segs):
            res.cell(row=r, column=11 + j,
                     value=f"=SUMIFS(Calculo!$Q$2:$Q${ultima},{rk},C{r},{rc},\"{s.segmento}\")").font = liga
        res.cell(row=r, column=11 + n, value=None if pd.isna(fila.ranking) else int(fila.ranking))
        res.cell(row=r, column=12 + n, value=int(fila.ranking_potencial)
                 if pd.notna(fila.ranking_potencial) else None)
        for col, formato in ([(4, NUM), (5, NUM), (6, USD), (7, USD), (8, USD), (9, USD), (10, PCT)]
                             + [(11 + j, USD) for j in range(n)]):
            res.cell(row=r, column=col).number_format = formato
    anchos(res, [30, 26, 10, 12, 12, 16, 16, 16, 16, 10] + [18] * n + [10, 10])
    res.freeze_panes = res.cell(row=primera, column=3)

    # --- Sensibilidad -------------------------------------------------------
    sen = wb.create_sheet("Sensibilidad")
    sen["A1"] = "Sensibilidad: margen de contribución total 2030 (US$ millones)"
    sen["A1"].font = titulo
    sen["A2"] = "Kilos del mercado 2030 por segmento (de Calculo); el resto de los supuestos sale de Supuestos."
    sen["A2"].font = gris
    encabezados(sen, 3, ["Segmento", "kg mercado 2030"])
    for i, s in enumerate(segs, start=4):
        sen.cell(row=i, column=1, value=s.nombre).font = normal
        celda = sen.cell(row=i, column=2,
                         value=f"=SUMIFS(Calculo!$J$2:$J${ultima},{rc},\"{s.segmento}\")")
        celda.font, celda.number_format = liga, NUM
    k0, k1 = 4, 3 + n
    desc = f"Supuestos!$F${f0}:$F${f1}"
    serv = f"Supuestos!$H${f0}:$H${f1}"

    def rejilla(fila0, titulo_tabla, eje_filas, valores_filas, formato_filas, valores_cols,
                formula):
        sen.cell(row=fila0, column=1, value=titulo_tabla).font = negrita
        sen.cell(row=fila0 + 1, column=1, value=f"{eje_filas} ↓ · Participación →").font = gris
        for j, v in enumerate(valores_cols, start=2):
            c = sen.cell(row=fila0 + 1, column=j, value=v)
            c.font, c.number_format, c.fill = entrada, PCT, encabezado_fondo
        for i, v in enumerate(valores_filas, start=fila0 + 2):
            c = sen.cell(row=i, column=1, value=v)
            c.font, c.number_format, c.fill = entrada, formato_filas, encabezado_fondo
            for j in range(2, 2 + len(valores_cols)):
                col = get_column_letter(j)
                c = sen.cell(row=i, column=j, value=formula(f"$A{i}", f"{col}${fila0 + 1}"))
                c.number_format = '#,##0.0;(#,##0.0);"-"'
        return fila0 + 2 + len(valores_filas)

    participaciones = [0.20, 0.25, 0.30, 0.35, 0.40]
    precios = [round(1.80 + 0.10 * i, 2) for i in range(9)]
    siguiente = rejilla(
        k1 + 3, "Precio de lista vs participación", "Precio de lista (US$/kg)", precios, USD2,
        participaciones,
        lambda p, s: (f"={s}*SUMPRODUCT($B${k0}:$B${k1},{p}*(1-{desc})-Supuestos!$B$6"
                      f"-Supuestos!$B$7-{serv})/1000000"))
    costos = [round(1.00 + 0.10 * i, 2) for i in range(8)]
    rejilla(siguiente + 2, "Costo del producto importado vs participación",
            "Costo del producto (US$/kg)", costos, USD2, participaciones,
            lambda c, s: (f"={s}*SUMPRODUCT($B${k0}:$B${k1},Supuestos!$B$5*(1-{desc})-{c}"
                          f"-Supuestos!$B$7-{serv})/1000000"))
    anchos(sen, [34, 16, 12, 12, 12, 12])

    wb.calculation.fullCalcOnLoad = True
    wb.save(ruta)
    return ruta


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    asegurar_carpetas()
    config = cargar_config()
    cfg_modelo, cfg_seg, cfg_rent = (config["modelo"], config["modelo_segmentos"],
                                     config["rentabilidad"])
    versiones = [v["etiqueta"] for v in config["denue"]["versiones"]]
    inicio = time.perf_counter()

    necesarios = {n: PROCESSED / n for n in ["panel_municipios", "denue_establecimientos",
                                             "metropolis_municipios", "metropolis",
                                             "potencial_metropolis"]}
    faltan = [str(r) for r in necesarios.values() if not r.exists()]
    faltan += [str(r) for r in [TABLAS / "mercado_parametros.json"] if not r.exists()]
    if faltan:
        log.error(f"No existe {faltan}. Primero: python -m src.analysis.modelo_potencial")
        return 1

    from src.analysis.modelo_potencial import construir_ejemplos

    spark = crear_spark("DatosMasivos-Rentabilidad")
    panel = spark.read.parquet(str(necesarios["panel_municipios"])).toPandas()
    municipios = spark.read.parquet(str(necesarios["metropolis_municipios"])).toPandas()
    metropolis = spark.read.parquet(str(necesarios["metropolis"])).toPandas()
    potencial = spark.read.parquet(str(necesarios["potencial_metropolis"])).toPandas()
    denue = spark.read.parquet(str(necesarios["denue_establecimientos"]))
    nacional = ms.conteo_nacional(denue, versiones[-1])
    mercado = json.loads((TABLAS / "mercado_parametros.json").read_text(encoding="utf-8"))

    # 1. Modelo por segmento: compartido contra uno por segmento.
    ejemplos = construir_ejemplos(panel, municipios, metropolis, versiones, cfg_modelo)
    datos = ms.ejemplos_segmentos(ejemplos, panel, municipios, versiones, cfg_modelo, cfg_seg)
    metricas, ganador = ms.evaluar(datos, cfg_modelo, cfg_seg, versiones[-1])
    metricas.to_csv(SALIDAS["metricas"], index=False)
    for _, fila in metricas[metricas["segmento"] == "Todos"].iterrows():
        log.info(f"{fila.modelo:22s} | MAE {fila.mae:5.2f} pp | RMSE {fila.rmse:5.2f}")
    log.info(f"Gana el modelo {ganador}")

    proyeccion, _ = ms.proyectar(datos, ganador, cfg_modelo, cfg_seg)
    proyeccion = proyeccion.merge(potencial[["clave_metropoli", "metropoli"]],
                                  on="clave_metropoli", how="left")

    # 2. Rentabilidad del escenario.
    matriz, generales = matriz_supuestos(cfg_rent, mercado, nacional)
    detalle = calcular(proyeccion, matriz, generales)
    resumen = resumir(detalle, potencial)
    validar(detalle, resumen, proyeccion)

    detalle.to_csv(SALIDAS["tabla_segmentos"], index=False, encoding="utf-8")
    resumen.to_csv(SALIDAS["tabla_metropolis"], index=False, encoding="utf-8")
    SALIDAS["supuestos"].write_text(json.dumps(
        {"generales": generales, "segmentos": matriz.to_dict(orient="records"),
         "modelo_ganador": ganador, "escenario": "simulado"}, indent=2, ensure_ascii=False),
        encoding="utf-8")
    spark.createDataFrame(detalle).coalesce(1).write.mode("overwrite").parquet(str(SALIDAS["segmentos"]))
    spark.createDataFrame(resumen).coalesce(1).write.mode("overwrite").parquet(str(SALIDAS["metropolis"]))
    escribir_excel(detalle, resumen, matriz, generales, SALIDAS["excel"])

    # 3. El heatmap con la utilidad del escenario.
    from src.analysis import mapa_potencial
    potencial_csv = pd.read_csv(TABLAS / "potencial_metropolis.csv")
    metricas_modelo = pd.read_csv(TABLAS / "modelo_metricas.csv")
    importancia = (pd.read_csv(TABLAS / "modelo_importancia.csv").set_index("variable")["ganancia"]
                   if (TABLAS / "modelo_importancia.csv").exists() else pd.Series(dtype=float))
    mapa_potencial.generar(potencial_csv, metricas_modelo, mercado, importancia,
                           FIGURAS / "mapa_potencial.html",
                           rentabilidad={"resumen": resumen, "detalle": detalle,
                                         "matriz": matriz, "generales": generales,
                                         "metricas": metricas, "ganador": ganador})

    for _, s in matriz.iterrows():
        log.info(f"{s.nombre:30s} | {s.kg_por_local:8,.0f} kg/local | margen "
                 f"US${s.margen_usd_kg:.2f}/kg ({s.margen_pct:.0%})")
    for _, fila in resumen.head(10).iterrows():
        log.info(f"{fila.ranking_utilidad:2d}. {fila.metropoli:28s} utilidad 2030 "
                 f"US${fila.utilidad_proy_usd / 1e6:6.2f} M | margen {fila.margen_proy_pct:.0%}")
    log.info(f"Rentabilidad (escenario) de {len(resumen)} metropolis en "
             f"{time.perf_counter() - inicio:.1f} s -> {SALIDAS['excel']}")
    cerrar_spark(spark)
    return 0


if __name__ == "__main__":
    sys.exit(main())
