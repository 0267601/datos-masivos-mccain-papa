"""
Reglas de validacion de las fuentes del indice de inversion.

Usamos los mismos niveles que en src/validation/reglas.py (OK / AVISO / FALLA)
y las mismas ocho categorias de la rubrica, aplicadas al DENUE, al catalogo
de metropolis de CONAPO, a las importaciones de Comtrade y al indice.
"""

from pyspark.sql import functions as F

from src.ingestion.comtrade_extractor import periodos
from src.validation import reglas as r
from src.validation.reglas import AVISO, FALLA, OK, resultado

COLUMNAS_DENUE = {
    "version": "string", "fecha_corte": "date", "id": "string", "nombre_act": "string",
    "categoria": "string", "cadena": "string", "cve_mun5": "string",
    "personal_min": "int", "latitud": "double", "longitud": "double",
}
COLUMNAS_IMPORTACIONES = {
    "periodo": "string", "anio": "int", "mes": "int", "pais_codigo": "int",
    "valor_usd": "double", "kg": "double", "usd_kg": "double", "es_total": "boolean",
}
COLUMNAS_INDICE = {
    "ranking": "int", "clave_metropoli": "string", "metropoli": "string", "indice": "double",
}

# Mexico continental e islas, con margen.
LATITUD, LONGITUD = (14.0, 33.0), (-119.0, -86.0)


def validar_formato(df, columna, patron, tabla):
    malos = df.filter(F.col(columna).isNull() | ~F.col(columna).rlike(patron)).count()
    estado = OK if malos == 0 else FALLA
    return resultado(tabla, f"formato de {columna}", estado, malos, f"filas que no cumplen {patron}")


def reglas_denue(denue, config):
    cfg = config["denue"]
    res = []
    r.exigir_columnas(denue, COLUMNAS_DENUE)
    res.append(r.validar_columnas(denue, COLUMNAS_DENUE, "denue"))
    res.append(r.validar_tipos(denue, COLUMNAS_DENUE, "denue"))
    res += r.validar_nulos(denue, {"nombre_act": 1, "fecha_corte": 0, "latitud": 5,
                                   "personal_min": 5}, "denue")
    res.append(r.validar_duplicados(denue, ["version", "id"], "denue"))
    res.append(validar_formato(denue, "cve_mun5", r"^\d{5}$", "denue"))
    res.append(r.validar_rango(denue, "latitud", *LATITUD, "denue", nivel=AVISO))
    res.append(r.validar_rango(denue, "longitud", *LONGITUD, "denue", nivel=AVISO))

    # Temporal: todas las versiones configuradas, una fecha de corte por version.
    esperadas = sorted(v["etiqueta"] for v in cfg["versiones"])
    reales = sorted(x[0] for x in denue.select("version").distinct().collect())
    faltan = sorted(set(esperadas) - set(reales))
    res.append(resultado("denue", "versiones esperadas", FALLA if faltan else OK, len(faltan),
                         f"presentes {reales}; faltan {faltan or 'ninguna'}"))
    fechas = denue.groupBy("version").agg(F.countDistinct("fecha_corte").alias("n")).filter("n != 1")
    malas = fechas.count()
    res.append(resultado("denue", "una fecha de corte por version", FALLA if malas else OK,
                         malas, f"{malas} versiones con mas de una fecha"))

    # Volumen y contenido por version.
    por_version = {f["version"]: f for f in denue.groupBy("version").agg(
        F.count("*").alias("filas"),
        F.sum(F.col("cadena").isNotNull().cast("int")).alias("cadenas"),
        *[F.sum((F.col("categoria") == c).cast("int")).alias(c) for c in cfg["categorias"]],
    ).collect()}
    pocas = {v: f["filas"] for v, f in por_version.items() if f["filas"] < 300_000}
    res.append(resultado("denue", "volumen por version", FALLA if pocas else OK, len(pocas),
                         f"minimo 300,000 por version; debajo: {pocas or 'ninguna'}"))
    vacias = {v: c for v, f in por_version.items() for c in cfg["categorias_objetivo"]
              if not f[c]}
    res.append(resultado("denue", "categorias objetivo en cada version",
                         FALLA if vacias else OK, len(vacias),
                         f"version/categoria sin establecimientos: {vacias or 'ninguna'}"))
    sin_cadenas = [v for v, f in por_version.items() if f["cadenas"] < 100]
    res.append(resultado("denue", "cadenas detectadas", AVISO if sin_cadenas else OK,
                         len(sin_cadenas),
                         ", ".join(f"{v}: {f['cadenas']:,}" for v, f in sorted(por_version.items()))))
    return res


def reglas_metropolis(municipios, metropolis):
    res = []
    n = metropolis.count()
    res.append(resultado("metropolis", "92 metropolis de CONAPO", OK if n == 92 else AVISO, n,
                         f"{n} metropolis en el catalogo"))
    res.append(r.validar_duplicados(municipios, ["cve_mun5"], "metropolis_municipios"))
    res.append(validar_formato(municipios, "cve_mun5", r"^\d{5}$", "metropolis_municipios"))
    res.append(r.validar_rango(metropolis, "poblacion_2020", 1, 3e7, "metropolis"))
    huerfanas = (municipios.select("clave_metropoli").distinct()
                 .join(metropolis, "clave_metropoli", "left_anti").count())
    res.append(resultado("metropolis", "municipios con metropoli en el catalogo",
                         OK if huerfanas == 0 else FALLA, huerfanas,
                         f"{huerfanas} claves de metropoli sin ficha"))
    return res


def reglas_importaciones(importaciones, config):
    cfg = config["comtrade"]
    res = []
    r.exigir_columnas(importaciones, COLUMNAS_IMPORTACIONES)
    res.append(r.validar_tipos(importaciones, COLUMNAS_IMPORTACIONES, "importaciones"))
    res += r.validar_nulos(importaciones, {"valor_usd": 0, "kg": 5}, "importaciones")
    res.append(r.validar_duplicados(importaciones, ["periodo", "pais_codigo"], "importaciones"))
    res.append(r.validar_rango(importaciones.filter(~F.col("es_total")), "usd_kg", 0.3, 5.0,
                               "importaciones", nivel=AVISO))

    esperados = set(periodos(cfg["desde"], cfg["hasta"]))
    reales = {x[0] for x in importaciones.select("periodo").distinct().collect()}
    faltan = sorted(esperados - reales)
    res.append(resultado("importaciones", "meses esperados", AVISO if faltan else OK,
                         len(faltan), f"{len(reales)} de {len(esperados)} meses; "
                         f"faltan {faltan[:6]}{'...' if len(faltan) > 6 else ''}"))

    # El renglon "Mundo" debe coincidir con la suma de los paises (±5%).
    comparacion = (
        importaciones.groupBy("periodo")
        .agg(F.sum(F.when(F.col("es_total"), F.col("valor_usd"))).alias("mundo"),
             F.sum(F.when(~F.col("es_total"), F.col("valor_usd"))).alias("paises"))
        .filter(F.col("mundo").isNotNull())
        .filter(F.abs(F.col("paises") - F.col("mundo")) / F.col("mundo") > 0.05)
        .count()
    )
    res.append(resultado("importaciones", "total = suma de paises", OK if comparacion == 0 else AVISO,
                         comparacion, f"{comparacion} meses con diferencia mayor a 5%"))
    return res


def reglas_indice(indice, metropolis, config):
    res = []
    r.exigir_columnas(indice, COLUMNAS_INDICE)
    res.append(r.validar_tipos(indice, COLUMNAS_INDICE, "indice"))
    res.append(r.validar_rango(indice, "indice", 0, 100, "indice"))
    res.append(r.validar_duplicados(indice, ["clave_metropoli"], "indice"))
    n_indice, n_metropolis = indice.count(), metropolis.count()
    res.append(resultado("indice", "una fila por metropoli",
                         OK if n_indice == n_metropolis else AVISO, n_indice,
                         f"{n_indice} en el indice de {n_metropolis} metropolis"))
    pesos = config["indice"]["pesos"]
    suma = round(sum(pesos.values()), 6)
    res.append(resultado("indice", "pesos suman 1", OK if suma == 1 else FALLA, suma, str(pesos)))
    sin_base = indice.filter(F.col("crecimiento_anual").isNull()).count()
    res.append(resultado("indice", "metropolis con crecimiento calculable",
                         OK if sin_base == 0 else AVISO, sin_base,
                         f"{sin_base} sin restaurantes objetivo en la version inicial"))
    return res
