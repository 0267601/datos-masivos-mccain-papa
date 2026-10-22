"""
Reglas de validacion sobre DataFrames de Spark.

Cada regla devuelve un diccionario con el mismo formato:

    {"tabla": ..., "regla": ..., "estado": "OK" | "AVISO" | "FALLA",
     "valor": ..., "detalle": ...}

Separamos tres niveles a proposito:
    OK     -> la regla se cumple
    AVISO  -> algo raro que hay que explicar, pero no invalida el dataset
              (ej. un estado que cotiza en mayoreo y no aparece en SIAP)
    FALLA  -> el dataset no se debe usar (ej. duplicados en la llave)

Asi el pipeline puede seguir con avisos documentados y solo se detiene con
fallas reales. Las reglas no modifican los datos: solo cuentan.
"""

from pyspark.sql import functions as F

OK, AVISO, FALLA = "OK", "AVISO", "FALLA"


def resultado(tabla, regla, estado, valor=None, detalle=""):
    return {"tabla": tabla, "regla": regla, "estado": estado,
            "valor": valor, "detalle": detalle}


# --- 1. Existencia de columnas --------------------------------------------------

def exigir_columnas(df, requeridas):
    """Detenemos todo si faltan columnas: sin ellas ninguna otra regla aplica."""
    faltan = set(requeridas) - set(df.columns)
    if faltan:
        raise ValueError(f"Faltan columnas: {sorted(faltan)}")


def validar_columnas(df, requeridas, tabla):
    faltan = sorted(set(requeridas) - set(df.columns))
    if faltan:
        return resultado(tabla, "columnas requeridas", FALLA, len(faltan),
                         f"faltan {faltan}")
    return resultado(tabla, "columnas requeridas", OK, len(requeridas),
                     f"{len(requeridas)} columnas presentes")


# --- 2. Tipos de datos ----------------------------------------------------------

def validar_tipos(df, esperados, tabla):
    """esperados: {columna: 'int' | 'double' | 'date' | 'boolean' | 'string'}."""
    reales = dict(df.dtypes)
    distintos = {c: f"{reales.get(c)} (esperado {t})"
                 for c, t in esperados.items() if reales.get(c) != t}
    if distintos:
        return resultado(tabla, "tipos de datos", FALLA, len(distintos), str(distintos))
    return resultado(tabla, "tipos de datos", OK, len(esperados),
                     f"{len(esperados)} columnas con el tipo esperado")


# --- 3. Valores faltantes -------------------------------------------------------

def validar_nulos(df, maximos, tabla):
    """maximos: {columna: % maximo de nulos permitido}. Una regla por columna."""
    total = df.count()
    conteos = df.select(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in maximos]
    ).first().asDict()

    salida = []
    for columna, maximo in maximos.items():
        pct = 100 * (conteos[columna] or 0) / total if total else 0
        estado = OK if pct <= maximo else FALLA
        salida.append(resultado(tabla, f"nulos en {columna}", estado, round(pct, 2),
                                f"{conteos[columna] or 0:,} de {total:,} (max {maximo}%)"))
    return salida


# --- 4. Duplicados --------------------------------------------------------------

def validar_duplicados(df, llave, tabla):
    total = df.count()
    unicos = df.select(*llave).distinct().count()
    duplicados = total - unicos
    estado = OK if duplicados == 0 else FALLA
    return resultado(tabla, "duplicados en la llave", estado, duplicados,
                     f"llave {llave}")


# --- 5. Rangos validos ----------------------------------------------------------

def validar_rango(df, columna, minimo, maximo, tabla, nivel=FALLA):
    """Contamos valores fuera de [minimo, maximo]. Los nulos no cuentan aqui."""
    fuera = df.filter((F.col(columna) < minimo) | (F.col(columna) > maximo)).count()
    estado = OK if fuera == 0 else nivel
    return resultado(tabla, f"rango de {columna}", estado, fuera,
                     f"valores fuera de [{minimo}, {maximo}]")


def validar_orden(df, menor, mayor, tabla, nivel=FALLA, tolerancia=0.0):
    """Contamos filas donde menor > mayor (ej. cosechada > sembrada)."""
    fuera = df.filter(F.col(menor) > F.col(mayor) * (1 + tolerancia)).count()
    estado = OK if fuera == 0 else nivel
    return resultado(tabla, f"{menor} <= {mayor}", estado, fuera,
                     f"filas donde {menor} > {mayor}")


def validar_identidad_produccion(df, tabla, tolerancia=0.05):
    """produccion ~ cosechada x rendimiento. SIAP redondea, damos 5% de holgura."""
    calculada = F.col("sup_cosechada_ha") * F.col("rendimiento_ton_ha")
    base = df.filter((F.col("produccion_ton") > 0) & calculada.isNotNull())
    total = base.count()
    fuera = base.filter(
        F.abs(calculada - F.col("produccion_ton")) / F.col("produccion_ton") > tolerancia
    ).count()
    pct = 100 * fuera / total if total else 0
    estado = OK if pct == 0 else (AVISO if pct < 5 else FALLA)
    return resultado(tabla, "produccion = cosechada x rendimiento", estado, round(pct, 2),
                     f"{fuera} de {total} filas fuera de +-{int(tolerancia * 100)}%")


# --- 6. Consistencia temporal ---------------------------------------------------

def validar_anios(df, columna, esperados, tabla):
    """Los anios deben ser exactamente los configurados, sin huecos."""
    reales = sorted(r[0] for r in df.select(columna).distinct().collect())
    faltan = sorted(set(esperados) - set(reales))
    sobran = sorted(set(reales) - set(esperados))
    if faltan or sobran:
        return resultado(tabla, "anios esperados", FALLA, len(faltan) + len(sobran),
                         f"faltan {faltan}, sobran {sobran}")
    return resultado(tabla, "anios esperados", OK, len(reales),
                     f"{reales[0]}-{reales[-1]} sin huecos")


def validar_fechas(df, tabla, hoy):
    """Ninguna cotizacion en el futuro y la particion anio/mes coincide con la fecha."""
    futuras = df.filter(F.col("fecha") > F.lit(hoy)).count()
    mal_particionadas = df.filter(
        (F.year("fecha") != F.col("anio")) | (F.month("fecha") != F.col("mes"))
    ).count()
    problemas = futuras + mal_particionadas
    estado = OK if problemas == 0 else FALLA
    return resultado(tabla, "fechas consistentes", estado, problemas,
                     f"{futuras} en el futuro, {mal_particionadas} en la particion equivocada")


def validar_cobertura_mensual(df, tabla):
    """Cada variedad deberia cotizar todos los meses del periodo.

    Es AVISO y no FALLA: hay variedades que el SNIIM solo cotiza por temporada.
    """
    meses_total = df.select("anio", "mes").distinct().count()
    por_variedad = (
        df.groupBy("variedad")
        .agg(F.countDistinct("anio", "mes").alias("meses"))
        .collect()
    )
    incompletas = {r["variedad"]: r["meses"] for r in por_variedad if r["meses"] < meses_total}
    estado = OK if not incompletas else AVISO
    return resultado(tabla, "cobertura mensual por variedad", estado, len(incompletas),
                     f"de {meses_total} meses: {incompletas or 'todas completas'}")


# --- 7. Consistencia entre tablas -----------------------------------------------

def validar_estados_cruce(siap, sniim, tabla="siap x sniim"):
    """Estados de origen en SNIIM que no tienen produccion en SIAP.

    AVISO: el SNIIM reporta el origen declarado en el mercado, no el productor
    (lo documentamos en el E1), asi que puede pasar. Lo listamos para revisarlo.
    """
    estados_siap = siap.select("estado_llave").distinct()
    sin_produccion = (
        sniim.filter(~F.col("es_importacion"))
        .select("estado_origen_llave", "estado_origen").distinct()
        .join(estados_siap,
              sniim["estado_origen_llave"] == estados_siap["estado_llave"], "left_anti")
        .collect()
    )
    nombres = sorted(r["estado_origen"] for r in sin_produccion)
    estado = OK if not nombres else AVISO
    return resultado(tabla, "origenes SNIIM con produccion en SIAP", estado, len(nombres),
                     f"sin produccion en SIAP: {nombres or 'ninguno'}")


def validar_porcentajes(canal, tabla="canalizacion", tolerancia=0.01):
    """Las participaciones de cada anio deben sumar 100."""
    sumas = (
        canal.groupBy("anio")
        .agg(F.sum("pct_nacional").alias("nacional"), F.sum("pct_mayoreo").alias("mayoreo"))
        .collect()
    )
    malos = [r["anio"] for r in sumas
             if abs(r["nacional"] - 100) > tolerancia or abs(r["mayoreo"] - 100) > tolerancia]
    estado = OK if not malos else FALLA
    return resultado(tabla, "participaciones suman 100%", estado, len(malos),
                     f"anios con suma distinta de 100: {malos or 'ninguno'}")


# --- 8. Volumen esperado --------------------------------------------------------

def validar_volumen_por_anio(df, columna, minimo, maximo, tabla):
    conteos = {r[columna]: r["count"] for r in df.groupBy(columna).count().collect()}
    fuera = {a: n for a, n in conteos.items() if not minimo <= n <= maximo}
    estado = OK if not fuera else FALLA
    return resultado(tabla, "volumen por anio", estado, len(fuera),
                     f"rango esperado [{minimo}, {maximo}]; fuera: {fuera or 'ninguno'}")


def validar_volumen_minimo(df, minimo, tabla):
    total = df.count()
    estado = OK if total >= minimo else FALLA
    return resultado(tabla, "volumen minimo", estado, total, f"minimo esperado {minimo:,}")


def validar_truncamiento(manifiesto, tabla="sniim raw"):
    """Ninguna particion del SNIIM debe haber tocado el limite de 1000 filas."""
    if manifiesto is None:
        return resultado(tabla, "particiones truncadas", AVISO, None,
                         "sin manifiesto (datos de muestra): no se puede revisar")
    sniim = [m for m in manifiesto["archivos"].values() if m.get("fuente") == "SNIIM"]
    truncadas = sum(1 for m in sniim if m.get("truncado"))
    errores = sum(1 for m in manifiesto["archivos"].values() if m.get("estado") == "error")
    estado = OK if truncadas == 0 and errores == 0 else FALLA
    return resultado(tabla, "particiones truncadas o con error", estado, truncadas + errores,
                     f"{truncadas} truncadas, {errores} con error de descarga")
