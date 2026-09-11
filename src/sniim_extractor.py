"""
Extractor de precios de papa — SNIIM (Secretaria de Economia)

Obtenemos esta fuente mediante scraping con requests y BeautifulSoup.

El problema de escala que resolvemos aqui:
    SNIIM corta cada consulta a 1000 registros y NO avisa que trunco.
    Es el mismo problema que encontramos con el limite de una API, asi que
    lo resolvemos igual: particionamos la consulta por periodo. Pedimos un
    mes a la vez y marcamos cada particion que toque el limite.

Pipeline que seguimos:
    URL -> HTML -> BeautifulSoup -> registros -> DataFrame -> CSV
"""

import calendar
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

URL_CONSULTA = (
    "http://www.economia-sniim.gob.mx/nuevo/Consultas/MercadosNacionales/"
    "PreciosDeMercado/Agricolas/ResultadosConsultaFechaFrutasYHortalizas.aspx"
)

# Tomamos los ProductoId del catalogo del SNIIM. Nos interesan porque
# separan la papa por variedad, cosa que SIAP no hace.
VARIEDADES_PAPA = {
    740: "Alpha",
    748: "Galeana",
    749: "Gema",
    753: "Marciana",
    766: "Rouset",
    767: "San Jose",
}

LIMITE_REGISTROS = 1000

COLUMNAS = ["fecha", "presentacion", "origen", "destino",
            "precio_min", "precio_max", "precio_frec", "obs"]


def descargar_html(producto_id, anio, mes, timeout=90):
    """Descargamos el HTML de un mes para una variedad."""
    ultimo_dia = calendar.monthrange(anio, mes)[1]

    params = {
        "fechaInicio": f"01/{mes:02d}/{anio}",
        "fechaFinal": f"{ultimo_dia}/{mes:02d}/{anio}",
        "ProductoId": producto_id,
        "OrigenId": -1,
        "Origen": "Todos",
        "DestinoId": -1,
        "Destino": "Todos",
        "PreciosPorId": 2,
        "RegistrosPorPagina": LIMITE_REGISTROS,
    }

    respuesta = requests.get(URL_CONSULTA, params=params, timeout=timeout)
    respuesta.raise_for_status()

    # Fijamos la codificacion a proposito: si la dejamos que la adivine,
    # los acentos se rompen y "Mexico" llega mal escrito.
    respuesta.encoding = "utf-8"

    return respuesta.text


def extraer_registros(html, variedad):
    """Convertimos el HTML del SNIIM en una lista de diccionarios."""
    soup = BeautifulSoup(html, "html.parser")

    # Buscamos la tabla de datos: es la unica cuyo encabezado dice "Fecha".
    # No podemos tomar la mas grande porque la pagina trae tablas de diseno.
    tabla = None
    for candidata in soup.find_all("table"):
        encabezado = candidata.find("tr")
        if encabezado and "Fecha" in encabezado.get_text():
            tabla = candidata
            break

    if tabla is None:
        return []

    registros = []

    # Saltamos las dos primeras filas porque son encabezado.
    for fila in tabla.find_all("tr")[2:]:
        celdas = [c.get_text(strip=True) for c in fila.find_all("td")]

        if len(celdas) < 7 or not celdas[0]:
            continue

        registros.append({
            "fecha": celdas[0],
            "presentacion": celdas[1],
            "origen": celdas[2],
            "destino": celdas[3],
            "precio_min": celdas[4],
            "precio_max": celdas[5],
            "precio_frec": celdas[6],
            "obs": celdas[7] if len(celdas) > 7 else "",
            "variedad": variedad,
        })

    return registros


def limpiar(df):
    """Asignamos los tipos correctos y derivamos dos columnas que usamos despues."""
    df = df.copy()

    df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors="coerce")

    for columna in ["precio_min", "precio_max", "precio_frec"]:
        df[columna] = pd.to_numeric(df[columna], errors="coerce")

    # El destino viene como "Estado: Nombre del mercado". Separamos el estado.
    df["estado_destino"] = df["destino"].str.split(":").str[0].str.strip()

    # Marcamos la papa importada: es la distincion central de nuestro analisis.
    df["es_importacion"] = df["origen"].eq("Importación")

    return df.dropna(subset=["fecha"]).reset_index(drop=True)


def descargar_periodo(producto_ids, anios, pausa=0.5, verbose=True):
    """Descargamos varias variedades y anios, un mes a la vez.

    Devolvemos (DataFrame, metricas). En metricas documentamos cada particion
    y marcamos las que tocaron el limite de 1000 registros, para saber si
    tendriamos que partir todavia mas fino.
    """
    registros = []
    metricas = []

    for producto_id in producto_ids:
        variedad = VARIEDADES_PAPA.get(producto_id, str(producto_id))

        for anio in anios:
            for mes in range(1, 13):

                html = descargar_html(producto_id, anio, mes)
                del_mes = extraer_registros(html, variedad)
                registros.extend(del_mes)

                metricas.append({
                    "variedad": variedad,
                    "anio": anio,
                    "mes": mes,
                    "filas": len(del_mes),
                    "html_mb": len(html) / 1024**2,
                    "truncado": len(del_mes) >= LIMITE_REGISTROS - 2,
                })

                if verbose:
                    print(f"{variedad:9s} {anio}-{mes:02d} | {len(del_mes):5,} filas")

                # Esperamos entre peticiones para no saturar el servidor.
                time.sleep(pausa)

    df = limpiar(pd.DataFrame(registros, columns=COLUMNAS + ["variedad"]))

    return df, pd.DataFrame(metricas)
