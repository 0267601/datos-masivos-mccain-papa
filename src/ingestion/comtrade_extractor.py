"""
Extractor de UN Comtrade — importaciones de Mexico de papas congeladas.

Obtenemos esta fuente de la API publica de UN Comtrade, que no pide llave.
Producto: HS 200410 (papas preparadas o conservadas, congeladas: la papa a la
francesa que compra la industria de comida rapida). Por cada mes y pais de
origen trae el valor en dolares y el peso neto en kilos, asi que podemos
calcular el precio por kilo de cada origen.

El limite que resolvemos:
    La API publica solo acepta un periodo por consulta. Igual que con el
    tope de 1000 registros del SNIIM, particionamos: pedimos un mes a la vez,
    con pausa entre peticiones, y reintentamos ante 429 o 5xx.

Por que no Data Mexico:
    Su API responde los metadatos, pero las consultas de datos regresaron
    502 en todas nuestras pruebas. Comtrade ademas trae kilos.

Pipeline:
    API -> requests -> JSON por mes -> data/raw/comtrade/<YYYYMM>.json
"""

import time

import requests

# Respaldo por si la API no manda la descripcion del pais.
PAISES = {
    0: "Mundo", 842: "Estados Unidos", 124: "Canada", 56: "Belgica",
    528: "Paises Bajos", 616: "Polonia", 276: "Alemania", 251: "Francia",
    32: "Argentina", 156: "China", 380: "Italia", 724: "Espana",
}


def periodos(desde, hasta):
    """Lista de meses YYYYMM entre dos fechas "YYYY-MM", inclusive."""
    anio, mes = map(int, desde.split("-"))
    fin = tuple(map(int, hasta.split("-")))
    salida = []
    while (anio, mes) <= fin:
        salida.append(f"{anio}{mes:02d}")
        mes += 1
        if mes == 13:
            anio, mes = anio + 1, 1
    return salida


def consultar_mes(cfg, periodo, intentos=4, espera=5):
    """Pedimos un mes. Reintentamos con espera creciente ante 429 y 5xx."""
    params = {
        "reporterCode": cfg["reportero"],
        "period": periodo,
        "cmdCode": cfg["producto"],
        "flowCode": cfg["flujo"],
        "includeDesc": "true",
    }
    for intento in range(1, intentos + 1):
        try:
            respuesta = requests.get(cfg["url"], params=params, timeout=cfg["timeout"])
            if respuesta.status_code == 429 or respuesta.status_code >= 500:
                raise requests.HTTPError(f"HTTP {respuesta.status_code}")
            respuesta.raise_for_status()
            datos = respuesta.json()
            if datos.get("error"):
                raise ValueError(datos["error"])
            return datos
        except (requests.RequestException, ValueError) as error:
            if intento == intentos:
                raise RuntimeError(f"Comtrade fallo {intentos} veces para {periodo}: {error}") from error
            time.sleep(espera * intento)


def registros(datos):
    """Del JSON de un mes nos quedamos con lo que usamos, como diccionarios."""
    salida = []
    for fila in datos.get("data", []):
        # Solo el agregado: si la API manda desgloses por aduana, transporte o
        # segundo socio, los saltamos para no contar doble.
        if (fila.get("customsCode") not in (None, "C00") or fila.get("motCode") not in (None, 0)
                or fila.get("partner2Code") not in (None, 0)):
            continue
        codigo = fila.get("partnerCode")
        salida.append({
            "periodo": str(fila.get("period")),
            "pais_codigo": codigo,
            "pais": fila.get("partnerDesc") or PAISES.get(codigo, str(codigo)),
            "valor_usd": fila.get("primaryValue"),
            "kg": fila.get("netWgt"),
        })
    return salida
