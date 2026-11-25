"""
Pipeline completo en una sola llamada: adquisicion -> Silver -> validacion
-> Gold -> modelo -> rentabilidad (escenario).

Ejecucion desde la raiz del repositorio:

    python main.py                      # todo: descarga, procesa, valida, analiza
    python main.py --sin-descarga       # usa lo que ya esta en data/raw
    python main.py --fuente indice      # solo el indice de inversion y el modelo

Seguimos la estructura de la clase de automatizacion de pipelines:

    leer_configuracion   parametros explicitos (argumentos o widgets en Databricks)
    ejecutar_etapa       corre una etapa, mide su duracion y registra OK o ERROR
    ejecutar_pipeline    encadena las etapas; si una falla, las siguientes no corren

Cada etapa llama al main() de su modulo, que tambien se puede correr solo.
Si la validacion encuentra una FALLA, el pipeline se detiene antes de Gold:
no publicamos un indice construido sobre datos que no pasaron las reglas.

El registro de cada corrida queda en reports/tables/registro_pipeline.json.

Pipeline:
    01_adquisicion -> 02_construir_silver -> 03_validar_silver
        -> 04_construir_gold -> 05_modelo_potencial -> 06_rentabilidad_escenario
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

from src.utils.registro import configurar_registro
from src.utils.rutas import SAMPLE, TABLAS, asegurar_carpetas

log = configurar_registro("pipeline")


def leer_configuracion(argv=None):
    parser = argparse.ArgumentParser(description="Pipeline completo del proyecto")
    parser.add_argument("--fuente", choices=["e1", "indice", "todas"], default="todas",
                        help="e1 = SIAP + SNIIM; indice = DENUE + CONAPO + Comtrade + modelo")
    parser.add_argument("--sin-descarga", action="store_true",
                        help="no descargar; usar lo que ya esta en data/raw")
    parser.add_argument("--muestra", action="store_true",
                        help="reconstruir data/raw con la muestra del repositorio (sin internet)")
    parser.add_argument("--permitir-fallas", action="store_true",
                        help="seguir aunque la validacion tenga FALLA (solo con datos de prueba)")
    args = parser.parse_args(argv or [])
    return {"fuente": args.fuente, "sin_descarga": args.sin_descarga,
            "muestra": args.muestra, "permitir_fallas": args.permitir_fallas}


def ejecutar_etapa(nombre, funcion, registro, permitir_fallas=False):
    """Corremos una etapa y dejamos evidencia: estado, duracion y error."""
    inicio = datetime.now(timezone.utc).isoformat()
    reloj = perf_counter()
    evento = {"etapa": nombre, "inicio_utc": inicio}
    try:
        codigo = funcion()
        if codigo not in (0, None) and not permitir_fallas:
            raise RuntimeError(f"{nombre} termino con codigo {codigo}")
        evento.update(estado="OK" if codigo in (0, None) else "OK_CON_FALLAS",
                      codigo=codigo, error=None)
        return codigo
    except Exception as exc:
        evento.update(estado="ERROR", codigo=None,
                      error=f"{type(exc).__name__}: {str(exc)[:1000]}")
        raise
    finally:
        evento["duracion_segundos"] = round(perf_counter() - reloj, 3)
        registro.append(evento)
        print(json.dumps(evento, ensure_ascii=False))


def etapas(cfg):
    """Lista ordenada (nombre, funcion). Importamos adentro para arrancar rapido."""
    from src.analysis import indice_inversion, modelo_potencial, rentabilidad
    from src.ingestion import download_data
    from src.processing import build_dataset
    from src.validation import validate_dataset

    fuente = cfg["fuente"]
    lista = []
    if cfg["muestra"]:
        lista.append(("01_adquisicion", lambda: download_data.main(["--muestra"])))
    elif not cfg["sin_descarga"]:
        lista.append(("01_adquisicion", lambda: download_data.main(["--fuente", fuente])))
    lista.append(("02_construir_silver", lambda: build_dataset.main(["--fuente", fuente])))
    lista.append(("03_validar_silver", validate_dataset.main))
    sin_muestra_indice = cfg["muestra"] and not list((SAMPLE / "indice").glob("denue/*/*.zip"))
    if fuente in ("indice", "todas") and sin_muestra_indice:
        log.warning("La muestra del indice no esta en data/sample/indice/: "
                    "con --muestra solo corremos el E1")
    elif fuente in ("indice", "todas"):
        lista.append(("04_construir_gold", indice_inversion.main))
        lista.append(("05_modelo_potencial", modelo_potencial.main))
        lista.append(("06_rentabilidad_escenario", rentabilidad.main))
    return lista


def ejecutar_pipeline(cfg):
    asegurar_carpetas()
    run_id = uuid4().hex[:16]
    registro = []
    estado_final = "ERROR"
    print("INICIO PIPELINE", run_id)
    try:
        for nombre, funcion in etapas(cfg):
            # Con la muestra, las reglas de volumen fallan por diseno (son pocas
            # filas): solo en ese caso dejamos seguir despues de la validacion.
            permitir = (cfg["permitir_fallas"] or cfg["muestra"]) and nombre == "03_validar_silver"
            ejecutar_etapa(nombre, funcion, registro, permitir_fallas=permitir)
        estado_final = "OK"
        return {"run_id": run_id, "estado": estado_final, "etapas": registro}
    finally:
        resumen = {"run_id": run_id, "estado": estado_final, "parametros": cfg,
                   "fin_utc": datetime.now(timezone.utc).isoformat(), "etapas": registro}
        print("RESUMEN_PIPELINE", json.dumps(resumen, ensure_ascii=False))
        ruta = TABLAS / "registro_pipeline.json"
        historial = json.loads(ruta.read_text(encoding="utf-8")) if ruta.exists() else []
        historial.append(resumen)
        ruta.write_text(json.dumps(historial, indent=2, ensure_ascii=False), encoding="utf-8")


def main(argv=None):
    cfg = leer_configuracion(argv)
    try:
        resultado = ejecutar_pipeline(cfg)
    except Exception as error:
        log.error(f"Pipeline detenido: {error}")
        return 1
    log.info(f"Pipeline {resultado['run_id']} terminado: {len(resultado['etapas'])} etapas OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
