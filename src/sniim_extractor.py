"""
Extractor de precios de papa — SNIIM (Secretaria de Economia)

Obtenemos esta fuente con requests + BeautifulSoup.

Pipeline:
    WEB -> requests -> HTML -> BeautifulSoup -> registros -> pandas -> CSV

El problema de escala que resolvemos:
    SNIIM corta cada consulta a 1000 registros y no avisa que trunco. Igual que
    con el limite de una API, particionamos la consulta por periodo: pedimos un
    mes a la vez y marcamos cada particion que toque el limite.

Estructura del HTML que aprovechamos:
    <table id="tblResultados">
        <tr><td class="Datos2">03/03/2025</td> ... 8 celdas ... </tr>
Las 792 filas de datos usan la misma clase, asi que el selector es estable.
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

# ProductoId del catalogo del SNIIM. Nos interesa porque separa la papa por
# variedad, cosa que SIAP no hace.
VARIEDADES_PAPA = {
    740: "Alpha",
    748: "Galeana",
    749: "Gema",
    753: "Marciana",
    766: "Rouset",
    767: "San Jose",
}

LIMITE = 1000                      # tope de registros por consulta del SNIIM
TABLA_RESULTADOS = "table#tblResultados"
CELDAS_DATOS = "td.Datos2"
CAMPOS = ["fecha", "presentacion", "origen", "destino",
          "precio_min", "precio_max", "precio_frec", "obs"]


def texto_seguro(elemento):
    """Devolvemos el texto de un elemento, o None si no existe."""
    if elemento is None:
        return None
    return elemento.text.strip()


def construir_params(producto_id, anio, mes):
    """Armamos los parametros de la consulta para un mes completo."""
    ultimo_dia = calendar.monthrange(anio, mes)[1]
    return {
        "fechaInicio": f"01/{mes:02d}/{anio}",
        "fechaFinal": f"{ultimo_dia}/{mes:02d}/{anio}",
        "ProductoId": producto_id,
        "OrigenId": -1,
        "Origen": "Todos",
        "DestinoId": -1,
        "Destino": "Todos",
        "PreciosPorId": 2,
        "RegistrosPorPagina": LIMITE,
    }


def descargar_pagina(producto_id, anio, mes, timeout=90, intentos=4, espera=3):
    """Descargamos el HTML de un mes para una variedad.

    El SNIIM devuelve 503 de forma intermitente cuando lo consultamos seguido.
    Sin reintentos, una sola falla tumba la corrida completa, asi que
    reintentamos con espera creciente antes de rendirnos.
    """
    for intento in range(1, intentos + 1):
        try:
            response = requests.get(URL_CONSULTA,
                                    params=construir_params(producto_id, anio, mes),
                                    timeout=timeout)
            response.encoding = "utf-8"   # el sitio sirve utf-8; fijarlo evita mojibake
            response.raise_for_status()
            return response.text

        except requests.RequestException as error:
            if intento == intentos:
                raise RuntimeError(
                    f"SNIIM falló {intentos} veces para "
                    f"producto {producto_id}, {anio}-{mes:02d}: {error}"
                ) from error
            time.sleep(espera * intento)   # 3 s, luego 6 s, luego 9 s


def extraer_precios_mes(producto_id, anio, mes, variedad=None):
    """Extraemos los registros de un mes como lista de diccionarios.

    Equivale a extraer_libros_pagina() de la practica de scraping: una
    funcion que recibe una pagina y devuelve registros listos para pandas.
    """
    if variedad is None:
        variedad = VARIEDADES_PAPA.get(producto_id, str(producto_id))

    html = descargar_pagina(producto_id, anio, mes)
    soup = BeautifulSoup(html, "html.parser")

    tabla = soup.select_one(TABLA_RESULTADOS)
    if tabla is None:
        return [], len(html)

    resultados = []

    for fila in tabla.select("tr"):
        celdas = fila.select(CELDAS_DATOS)

        # Saltamos encabezados y filas incompletas.
        if len(celdas) != len(CAMPOS):
            continue

        registro = {campo: texto_seguro(celda)
                    for campo, celda in zip(CAMPOS, celdas)}
        registro["variedad"] = variedad
        resultados.append(registro)

    return resultados, len(html)


def limpiar(df):
    """Asignamos tipos y derivamos las dos columnas que usamos despues."""
    df = df.copy()

    df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors="coerce")

    for columna in ["precio_min", "precio_max", "precio_frec"]:
        df[columna] = pd.to_numeric(df[columna], errors="coerce")

    # El destino viene como "Estado: Nombre del mercado". Separamos el estado.
    df["estado_destino"] = df["destino"].str.split(":").str[0].str.strip()

    # Marcamos la papa importada: es la distincion central de nuestro analisis.
    df["es_importacion"] = df["origen"].eq("Importación")

    return df.dropna(subset=["fecha"]).reset_index(drop=True)


def descargar_periodo(producto_ids, anios, pausa=1.0, verbose=True):
    """Recorremos variedades y meses, y devolvemos (DataFrame, metricas).

    En metricas documentamos cada particion y marcamos las que tocaron el
    limite, para saber si tendriamos que partir todavia mas fino.
    """
    resultados = []
    metricas = []

    for producto_id in producto_ids:
        variedad = VARIEDADES_PAPA.get(producto_id, str(producto_id))

        for anio in anios:
            for mes in range(1, 13):

                del_mes, bytes_html = extraer_precios_mes(producto_id, anio, mes, variedad)
                resultados.extend(del_mes)

                metricas.append({
                    "variedad": variedad,
                    "anio": anio,
                    "mes": mes,
                    "filas": len(del_mes),
                    "html_mb": bytes_html / 1024**2,
                    "truncado": len(del_mes) >= LIMITE - 2,
                })

                if verbose:
                    print(f"{variedad:9s} {anio}-{mes:02d} | {len(del_mes):5,} filas")

                time.sleep(pausa)      # cortesia con el servidor

    df = limpiar(pd.DataFrame(resultados, columns=CAMPOS + ["variedad"]))

    return df, pd.DataFrame(metricas)
