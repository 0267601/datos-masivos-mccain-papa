"""
Adquisicion de datos crudos — SIAP, SNIIM, DENUE, CONAPO y UN Comtrade.

Ejecucion desde la terminal, en la raiz del repositorio:

    python -m src.ingestion.download_data                  # todas las fuentes
    python -m src.ingestion.download_data --fuente sniim   # solo una
    python -m src.ingestion.download_data --fuente indice  # DENUE + CONAPO + Comtrade
    python -m src.ingestion.download_data --forzar         # ignora la cache
    python -m src.ingestion.download_data --muestra        # sin internet (copias del E1)

--muestra se niega a sobrescribir una descarga real (ver copiar_muestra).

Que hacemos aqui y por que:

1. Guardamos una copia cruda, sin modificar, en la capa raw (data/raw/ en local,
   el Volume del data lake en Databricks):
       data/raw/siap/siap_cierre_<anio>.csv                  (CSV completo, latin-1)
       data/raw/sniim/<variedad>/sniim_<id>_<anio>-<mes>.html (HTML de la consulta)
       data/raw/denue/<version>/denue_..._csv.zip            (zip tal como lo da INEGI)
       data/raw/metropolis/<tabla>.csv                       (CSV de CONAPO)
       data/raw/comtrade/<YYYYMM>.json                       (respuesta de la API)
   No filtramos ni limpiamos nada: eso le toca a src/processing/. Si una
   transformacion sale mal, podemos rehacerla sin volver a pedirle nada a la
   fuente.

2. Evitamos descargas innecesarias: si el archivo ya existe, lo saltamos. Para
   que una descarga cortada no se quede como "cache", escribimos primero en un
   archivo temporal y lo renombramos al terminar.

3. Registramos errores sin tumbar la corrida: si una particion falla, la
   anotamos en logs/pipeline.log y en el manifiesto y seguimos con la siguiente.
   Al final regresamos codigo de salida 1 si hubo fallas.

4. Conservamos trazabilidad en data/raw/manifiesto.json: por cada archivo
   guardamos url, parametros, bytes, sha256, fecha y estado. En SNIIM ademas
   contamos las filas y marcamos si la particion toco el limite de 1000.

5. No usamos credenciales: ninguna de las fuentes las pide.

Pipeline:
    fuente -> requests -> bytes/HTML -> data/raw/ -> manifiesto.json
"""

import argparse
import hashlib
import json
import sys
import time

import requests
from datetime import date, datetime

from src.ingestion import comtrade_extractor, denue_extractor, siap_extractor, sniim_extractor
from src.utils.config import cargar_config
from src.utils.registro import configurar_registro
from src.utils.rutas import RAW, asegurar_carpetas

MANIFIESTO = RAW / "manifiesto.json"

log = configurar_registro("ingestion")


# --- Utilidades de archivo ------------------------------------------------------

def slug(texto):
    """Convertimos 'San Jose' en 'san_jose' para usarlo como carpeta."""
    return texto.strip().lower().replace(" ", "_")


def sha256(contenido):
    return hashlib.sha256(contenido).hexdigest()


def escribir_atomico(ruta, contenido):
    """Escribimos en <ruta>.parcial y renombramos al final.

    Si la descarga se corta a la mitad, nunca queda un archivo incompleto con
    el nombre final, asi que la cache no lo confunde con uno bueno.
    """
    ruta.parent.mkdir(parents=True, exist_ok=True)
    temporal = ruta.with_suffix(ruta.suffix + ".parcial")
    temporal.write_bytes(contenido)
    try:
        temporal.replace(ruta)
    except OSError:
        # Algunos sistemas de archivos montados (como un Volume de Databricks)
        # no permiten renombrar encima de un archivo existente. Escribimos
        # directo y borramos el temporal.
        ruta.write_bytes(contenido)
        temporal.unlink(missing_ok=True)


def leer_manifiesto():
    if MANIFIESTO.exists():
        return json.loads(MANIFIESTO.read_text(encoding="utf-8"))
    return {"archivos": {}}


def guardar_manifiesto(manifiesto):
    manifiesto["actualizado"] = datetime.now().isoformat(timespec="seconds")
    MANIFIESTO.parent.mkdir(parents=True, exist_ok=True)
    MANIFIESTO.write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False),
                          encoding="utf-8")


def ruta_relativa(ruta):
    """Clave del manifiesto: la ruta dentro de la capa raw (siap/..., sniim/...).

    La tomamos relativa a raw y no al repositorio porque en Databricks la capa
    raw vive en un Volume, fuera de la carpeta del repo.
    """
    return ruta.relative_to(RAW).as_posix()


# --- SIAP -----------------------------------------------------------------------

def ruta_siap(anio):
    return RAW / "siap" / f"siap_cierre_{anio}.csv"


def descargar_siap(config, manifiesto, forzar=False):
    """Descargamos un CSV completo por anio. Devolvemos el numero de errores."""
    cfg = config["siap"]
    errores = 0

    for anio in cfg["anios"]:
        destino = ruta_siap(anio)
        clave = ruta_relativa(destino)

        if destino.exists() and destino.stat().st_size > 0 and not forzar:
            log.info(f"SIAP {anio} | en cache, no descargamos")
            continue

        try:
            inicio = time.perf_counter()
            contenido = siap_extractor.descargar_crudo(anio, timeout=cfg["timeout"])
            escribir_atomico(destino, contenido)

            manifiesto["archivos"][clave] = {
                "fuente": "SIAP",
                "url": siap_extractor.URL_DESCARGA,
                "params": {"view": siap_extractor.VISTA_MUNICIPAL, "ANIO": anio},
                "bytes": len(contenido),
                "sha256": sha256(contenido),
                "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                "segundos": round(time.perf_counter() - inicio, 2),
                "estado": "ok",
            }
            log.info(f"SIAP {anio} | {len(contenido) / 1024**2:5.1f} MB descargados")

        except Exception as error:          # registramos y seguimos con el siguiente
            errores += 1
            manifiesto["archivos"][clave] = {
                "fuente": "SIAP",
                "params": {"ANIO": anio},
                "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                "estado": "error",
                "error": str(error)[:300],
            }
            log.error(f"SIAP {anio} | fallo la descarga: {error}")

        guardar_manifiesto(manifiesto)      # guardamos en cada paso: trazabilidad
        time.sleep(cfg["pausa"])

    return errores


# --- SNIIM ----------------------------------------------------------------------

def ruta_sniim(producto_id, variedad, anio, mes):
    return RAW / "sniim" / slug(variedad) / f"sniim_{producto_id}_{anio}-{mes:02d}.html"


def meses_disponibles(anio):
    """Solo pedimos meses que ya pasaron: el SNIIM no tiene datos del futuro."""
    hoy = date.today()
    if anio < hoy.year:
        return range(1, 13)
    if anio == hoy.year:
        return range(1, hoy.month + 1)
    return range(0)


def descargar_sniim(config, manifiesto, forzar=False):
    """Descargamos una pagina HTML por variedad y mes. Devolvemos errores."""
    cfg = config["sniim"]
    errores = 0

    for producto_id, variedad in cfg["variedades"].items():
        for anio in cfg["anios"]:
            for mes in meses_disponibles(anio):
                destino = ruta_sniim(producto_id, variedad, anio, mes)
                clave = ruta_relativa(destino)

                if destino.exists() and destino.stat().st_size > 0 and not forzar:
                    continue

                try:
                    html = sniim_extractor.descargar_pagina(
                        producto_id, anio, mes,
                        timeout=cfg["timeout"], intentos=cfg["intentos"],
                    )
                    contenido = html.encode("utf-8")
                    escribir_atomico(destino, contenido)

                    filas = len(sniim_extractor.parsear_html(html, variedad))
                    truncado = filas >= sniim_extractor.LIMITE - 2

                    manifiesto["archivos"][clave] = {
                        "fuente": "SNIIM",
                        "url": sniim_extractor.URL_CONSULTA,
                        "params": sniim_extractor.construir_params(producto_id, anio, mes),
                        "variedad": variedad,
                        "bytes": len(contenido),
                        "sha256": sha256(contenido),
                        "filas": filas,
                        "truncado": truncado,
                        "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                        "estado": "ok",
                    }
                    nivel = log.warning if truncado else log.info
                    nivel(f"SNIIM {variedad:9s} {anio}-{mes:02d} | {filas:5,} filas"
                          + (" | TRUNCADO: hay que partir mas fino" if truncado else ""))

                except Exception as error:
                    errores += 1
                    manifiesto["archivos"][clave] = {
                        "fuente": "SNIIM",
                        "variedad": variedad,
                        "params": {"ProductoId": producto_id, "anio": anio, "mes": mes},
                        "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                        "estado": "error",
                        "error": str(error)[:300],
                    }
                    log.error(f"SNIIM {variedad} {anio}-{mes:02d} | fallo: {error}")

                guardar_manifiesto(manifiesto)
                time.sleep(cfg["pausa"])      # cortesia con el servidor

    return errores


# --- DENUE ----------------------------------------------------------------------

def ruta_denue(etiqueta, url):
    return RAW / "denue" / etiqueta / denue_extractor.nombre_archivo(url)


def descargar_denue(config, manifiesto, forzar=False):
    """Descargamos los zip del sector 72 de cada version configurada."""
    cfg = config["denue"]
    errores = 0

    for version in cfg["versiones"]:
        for url in version["urls"]:
            destino = ruta_denue(version["etiqueta"], url)
            clave = ruta_relativa(destino)

            if destino.exists() and destino.stat().st_size > 0 and not forzar:
                log.info(f"DENUE {version['etiqueta']} | {destino.name} en cache")
                continue

            try:
                inicio = time.perf_counter()
                total, huella = denue_extractor.descargar_zip(url, destino, cfg["timeout"])
                manifiesto["archivos"][clave] = {
                    "fuente": "DENUE",
                    "url": url,
                    "version": version["etiqueta"],
                    "bytes": total,
                    "sha256": huella,
                    "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                    "segundos": round(time.perf_counter() - inicio, 1),
                    "estado": "ok",
                }
                log.info(f"DENUE {version['etiqueta']} | {destino.name} | "
                         f"{total / 1024**2:6.1f} MB")
            except Exception as error:
                errores += 1
                manifiesto["archivos"][clave] = {
                    "fuente": "DENUE", "url": url, "version": version["etiqueta"],
                    "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                    "estado": "error", "error": str(error)[:300],
                }
                log.error(f"DENUE {version['etiqueta']} | {destino.name} | fallo: {error}")

            guardar_manifiesto(manifiesto)
            time.sleep(cfg["pausa"])

    return errores


# --- CONAPO: zonas metropolitanas ----------------------------------------------

ENCABEZADOS_NAVEGADOR = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
                   "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"),
    "Accept": "text/csv,application/octet-stream,*/*",
}


def descargar_metropolis(config, manifiesto, forzar=False):
    """Descargamos los CSV de Las metropolis de Mexico 2020."""
    cfg = config["metropolis"]
    errores = 0

    for tabla, url in cfg["urls"].items():
        destino = RAW / "metropolis" / f"{tabla}.csv"
        clave = ruta_relativa(destino)

        # Si alguien bajo el CSV a mano y lo subio con su nombre original, lo usamos.
        original = RAW / "metropolis" / url.rsplit("/", 1)[-1]
        if not destino.exists() and original.exists():
            escribir_atomico(destino, original.read_bytes())
            log.info(f"CONAPO {tabla} | tomado de {original.name} (subido a mano)")
            continue

        if destino.exists() and destino.stat().st_size > 0 and not forzar:
            log.info(f"CONAPO {tabla} | en cache")
            continue

        try:
            # datos.gob.mx responde 403 al agente por defecto de requests desde
            # algunas nubes (Databricks); nos presentamos como navegador.
            respuesta = requests.get(url, timeout=cfg["timeout"], headers=ENCABEZADOS_NAVEGADOR)
            respuesta.raise_for_status()
            contenido = respuesta.content
            if contenido.lstrip()[:15].lower().startswith((b"<!doctype", b"<html")):
                raise ValueError("la respuesta es una pagina HTML, no un CSV")
            escribir_atomico(destino, contenido)
            manifiesto["archivos"][clave] = {
                "fuente": "CONAPO", "url": url, "bytes": len(contenido),
                "sha256": sha256(contenido),
                "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                "estado": "ok",
            }
            log.info(f"CONAPO {tabla} | {len(contenido) / 1024:6.1f} KB")
        except Exception as error:
            errores += 1
            manifiesto["archivos"][clave] = {
                "fuente": "CONAPO", "url": url,
                "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                "estado": "error", "error": str(error)[:300],
            }
            log.error(f"CONAPO {tabla} | fallo: {error}")

        guardar_manifiesto(manifiesto)

    return errores


# --- UN Comtrade ----------------------------------------------------------------

def descargar_comtrade(config, manifiesto, forzar=False):
    """Descargamos un JSON por mes con las importaciones de HS 200410."""
    cfg = config["comtrade"]
    errores = 0

    for periodo in comtrade_extractor.periodos(cfg["desde"], cfg["hasta"]):
        destino = RAW / "comtrade" / f"{periodo}.json"
        clave = ruta_relativa(destino)

        if destino.exists() and destino.stat().st_size > 0 and not forzar:
            continue

        try:
            datos = comtrade_extractor.consultar_mes(cfg, periodo)
            filas = len(datos.get("data", []))
            if filas == 0:
                # Mes sin datos (todavia no publicado): no lo guardamos para
                # que la siguiente corrida lo vuelva a pedir.
                log.warning(f"Comtrade {periodo} | sin datos todavia")
            else:
                contenido = json.dumps(datos, ensure_ascii=False).encode("utf-8")
                escribir_atomico(destino, contenido)
                manifiesto["archivos"][clave] = {
                    "fuente": "COMTRADE", "url": cfg["url"],
                    "params": {"reporterCode": cfg["reportero"], "period": periodo,
                               "cmdCode": cfg["producto"], "flowCode": cfg["flujo"]},
                    "bytes": len(contenido), "sha256": sha256(contenido),
                    "filas": filas,
                    "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                    "estado": "ok",
                }
                log.info(f"Comtrade {periodo} | {filas:2d} paises")
        except Exception as error:
            errores += 1
            manifiesto["archivos"][clave] = {
                "fuente": "COMTRADE", "params": {"period": periodo},
                "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
                "estado": "error", "error": str(error)[:300],
            }
            log.error(f"Comtrade {periodo} | fallo: {error}")

        guardar_manifiesto(manifiesto)
        time.sleep(cfg["pausa"])

    return errores


# --- Muestra sin internet -------------------------------------------------------

def hay_descarga_real():
    """Revisamos si data/raw/ ya tiene una descarga real de las fuentes.

    Dos senales, porque cualquiera de las dos puede faltar:
    - el manifiesto tiene al menos un archivo descargado con exito;
    - algun CSV del SIAP pesa mas de 1 MB (el real trae todos los cultivos,
      unos 6 MB; el de la muestra solo trae papa, unos 30 KB).
    """
    manifiesto = leer_manifiesto()
    # Solo cuentan SIAP y SNIIM: la muestra no toca las demas fuentes.
    if any(m.get("estado") == "ok" and (m.get("fuente") in ("SIAP", "SNIIM")
                                        or clave.startswith(("siap/", "sniim/")))
           for clave, m in manifiesto["archivos"].items()):
        return True
    return any(ruta.stat().st_size > 1024**2
               for ruta in (RAW / "siap").glob("siap_cierre_*.csv"))


def copiar_muestra(forzar=False):
    """Reconstruimos data/raw/ desde las copias del E1 para correr sin red.

    Nos sirve para evaluar y para las pruebas cuando los portales estan caidos.
    Ver src/ingestion/muestra.py.

    Si data/raw/ ya tiene una descarga real, nos negamos: la muestra
    sobrescribiria la copia cruda, que es justo lo que debemos conservar sin
    modificar. Con forzar=True la sobrescribimos y borramos el manifiesto,
    porque ya no describiria los archivos que quedan.
    """
    from src.ingestion.muestra import construir_muestra

    if hay_descarga_real():
        if not forzar:
            log.error("data/raw/ ya tiene una descarga real: no la sobrescribimos con "
                      "la muestra. Para usar la que ya esta, corre el pipeline sin "
                      "--muestra. Para reemplazarla de todos modos: --muestra --forzar")
            return None
        log.warning("Sobrescribimos la descarga real con la muestra (--forzar). "
                    "Borramos el manifiesto porque ya no corresponde.")
        MANIFIESTO.unlink(missing_ok=True)

    conteo = construir_muestra()
    log.info(f"Muestra escrita en {RAW}: {conteo['siap']} CSV SIAP, "
             f"{conteo['sniim']} HTML SNIIM")
    return conteo


# --- Punto de entrada -----------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga de datos crudos")
    parser.add_argument("--fuente", default="todas",
                        choices=["siap", "sniim", "e1", "denue", "metropolis", "comtrade",
                                 "indice", "todas"],
                        help="e1 = siap + sniim; indice = denue + metropolis + comtrade")
    parser.add_argument("--forzar", action="store_true",
                        help="descargamos aunque el archivo ya exista; con --muestra, "
                             "permite sobrescribir una descarga real")
    parser.add_argument("--muestra", action="store_true",
                        help="reconstruimos data/raw desde las copias del E1, sin internet")
    args = parser.parse_args(argv)

    asegurar_carpetas()

    if args.muestra:
        return 0 if copiar_muestra(forzar=args.forzar) else 1

    config = cargar_config()
    manifiesto = leer_manifiesto()
    inicio = time.perf_counter()
    errores = 0

    grupos = {
        "siap": (descargar_siap, {"siap", "e1", "todas"}),
        "sniim": (descargar_sniim, {"sniim", "e1", "todas"}),
        "metropolis": (descargar_metropolis, {"metropolis", "indice", "todas"}),
        "comtrade": (descargar_comtrade, {"comtrade", "indice", "todas"}),
        "denue": (descargar_denue, {"denue", "indice", "todas"}),
    }
    for funcion, cuando in grupos.values():
        if args.fuente in cuando:
            errores += funcion(config, manifiesto, args.forzar)

    log.info(f"Adquisicion terminada en {time.perf_counter() - inicio:.1f} s "
             f"| errores: {errores}")

    if errores:
        log.error("Hubo particiones con error. Revisar logs/pipeline.log y "
                  "volver a correr: las que ya bajaron se toman de la cache.")
    return 1 if errores else 0


if __name__ == "__main__":
    sys.exit(main())
