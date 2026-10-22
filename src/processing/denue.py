"""
Procesamiento del DENUE con PySpark: raw -> interim -> processed.

raw -> interim
    Spark no lee dentro de un zip, asi que primero extraemos el CSV de datos
    de cada zip a interim/_csv_denue/<version>/ (sin modificarlo). Luego Spark
    lee cada version con su propia codificacion y unificamos encabezados:

        2015       "Clave municipio", "Nombre de la Unidad Economica"... UTF-8
        2019-2026  "cve_mun", "nom_estab"...                           Latin-1

    Igual que con el SIAP, no podemos leer todas las versiones de un jalon:
    leemos version por version, renombramos y juntamos con unionByName.
    En interim dejamos las columnas que usamos, todo como texto, mas la
    version y el archivo de origen.

interim -> processed
    Clave de municipio a 5 digitos, categoria del negocio (comida rapida,
    autoservicio, a la carta, bares, hoteles), cadena (McDonald's, KFC...),
    personal ocupado, coordenadas como numero y fecha de corte de la version.
    Escribimos Parquet particionado por version.

Pipeline:
    zip -> CSV extraido -> Spark (por version) -> unionByName -> Parquet interim
        -> categorias/cadenas/tipos -> Parquet processed (version=)
"""

import unicodedata
import zipfile
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Columnas que usamos, con su nombre corto (el de 2019 en adelante).
COLUMNAS = ["id", "nom_estab", "raz_social", "codigo_act", "nombre_act", "per_ocu",
            "cve_ent", "entidad", "cve_mun", "municipio", "latitud", "longitud",
            "fecha_alta"]

# Encabezados largos de 2015 (ya normalizados: minusculas y sin acentos).
ALIAS = {
    "nombre de la unidad economica": "nom_estab",
    "razon social": "raz_social",
    "codigo de la clase de actividad scian": "codigo_act",
    "nombre de clase de la actividad": "nombre_act",
    "descripcion estrato personal ocupado": "per_ocu",
    "clave entidad": "cve_ent",
    "entidad federativa": "entidad",
    "clave municipio": "cve_mun",
    "fecha de incorporacion al denue": "fecha_alta",
}

ACENTOS, SIN_ACENTOS = "ÁÉÍÓÚÜÑáéíóúüñ", "AEIOUUNaeiouun"


def normalizar_nombre(nombre):
    """'Clave municipio' -> 'clave municipio'; quita BOM, comillas y acentos."""
    limpio = nombre.replace("﻿", "").strip().strip('"').strip()
    sin_acentos = "".join(c for c in unicodedata.normalize("NFD", limpio)
                          if unicodedata.category(c) != "Mn")
    clave = sin_acentos.lower()
    return ALIAS.get(clave, clave)


# --- raw -> interim -------------------------------------------------------------

def es_csv_de_datos(nombre):
    """Dentro del zip: el CSV de datos, no el diccionario ni los metadatos."""
    nombre = nombre.lower()
    return nombre.endswith(".csv") and "diccionario" not in nombre and "metadato" not in nombre


def extraer_csvs(carpeta_zip, carpeta_destino):
    """Extraemos el CSV de datos de cada zip. Si ya esta extraido, lo saltamos."""
    carpeta_destino.mkdir(parents=True, exist_ok=True)
    extraidos = []

    for ruta_zip in sorted(carpeta_zip.glob("*.zip")):
        with zipfile.ZipFile(ruta_zip) as archivo:
            entrada = next(i for i in archivo.infolist() if es_csv_de_datos(i.filename))
            destino = carpeta_destino / f"{ruta_zip.stem}.csv"
            if not (destino.exists() and destino.stat().st_size == entrada.file_size):
                with archivo.open(entrada) as origen, open(destino, "wb") as salida:
                    while bloque := origen.read(8 * 1024 * 1024):
                        salida.write(bloque)
            extraidos.append(destino)

    return extraidos


def detectar_codificacion(ruta, muestra=2 * 1024 * 1024):
    """UTF-8 si los primeros MB se decodifican sin error; si no, Latin-1."""
    with open(ruta, "rb") as archivo:
        bloque = archivo.read(muestra)
    corte = bloque.rfind(b"\n")             # no partimos un caracter a la mitad
    try:
        bloque[: corte if corte > 0 else len(bloque)].decode("utf-8")
        return "UTF-8"
    except UnicodeDecodeError:
        return "ISO-8859-1"


def leer_version(spark, rutas_csv, etiqueta):
    """Leemos los CSV de una version y unificamos su esquema."""
    codificacion = detectar_codificacion(rutas_csv[0])
    df = (
        spark.read
        .option("header", True)
        .option("encoding", codificacion)
        .option("multiLine", True)          # hay direcciones con saltos de linea
        .option("escape", '"')
        .option("inferSchema", False)
        .csv([str(r) for r in rutas_csv])
    )
    df = df.toDF(*[normalizar_nombre(c) for c in df.columns])

    for columna in COLUMNAS:
        if columna not in df.columns:
            df = df.withColumn(columna, F.lit(None).cast("string"))

    return (
        df.select(*COLUMNAS)
        .withColumn("version", F.lit(etiqueta))
        .withColumn("archivo", F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1))
    )


def construir_interim(spark, carpeta_raw, carpeta_csv, versiones):
    """Extraemos y juntamos todas las versiones configuradas que esten en raw."""
    partes = []
    for version in versiones:
        etiqueta = version["etiqueta"]
        carpeta_zip = carpeta_raw / etiqueta
        if not list(carpeta_zip.glob("*.zip")):
            continue
        rutas = extraer_csvs(carpeta_zip, carpeta_csv / etiqueta)
        partes.append(leer_version(spark, rutas, etiqueta))

    if not partes:
        raise FileNotFoundError(
            f"No hay zip del DENUE en {carpeta_raw}. "
            "Primero: python -m src.ingestion.download_data --fuente denue"
        )
    return reduce(DataFrame.unionByName, partes)


# --- interim -> processed -------------------------------------------------------

def sin_acentos(columna):
    return F.translate(columna, ACENTOS, SIN_ACENTOS)


def columna_categoria(categorias):
    """Primera categoria cuyas palabras aparecen en el nombre de la actividad."""
    actividad = F.lower(sin_acentos(F.col("nombre_act")))
    expr = F.lit("otros")
    for categoria, palabras in reversed(list(categorias.items())):
        condicion = reduce(lambda a, b: a | b, [actividad.contains(p) for p in palabras])
        expr = F.when(condicion, F.lit(categoria)).otherwise(expr)
    return expr


def columna_cadena(cadenas):
    """Nombre de la cadena si el nombre del establecimiento coincide; si no, null."""
    nombre = F.upper(sin_acentos(F.coalesce(F.col("nom_estab"), F.lit(""))))
    expr = F.lit(None).cast("string")
    for cadena, patron in reversed(list(cadenas.items())):
        expr = F.when(nombre.rlike(patron), F.lit(cadena)).otherwise(expr)
    return expr


def construir_processed(df_interim, cfg):
    """Tipos, claves, categorias y cadenas. Devolvemos (df, metricas)."""
    metricas = {"filas_interim": df_interim.count()}

    df = df_interim
    for columna in COLUMNAS:
        df = df.withColumn(columna, F.trim(F.col(columna)))

    fechas = F.create_map(*[x for v in cfg["versiones"]
                            for x in (F.lit(v["etiqueta"]), F.lit(v["fecha"]))])

    df = (
        df
        .withColumn("cve_mun5", F.when(
            F.col("cve_ent").rlike(r"^\d{1,2}$") & F.col("cve_mun").rlike(r"^\d{1,3}$"),
            F.concat(F.lpad("cve_ent", 2, "0"), F.lpad("cve_mun", 3, "0"))))
        .withColumn("categoria", columna_categoria(cfg["categorias"]))
        .withColumn("cadena", columna_cadena(cfg["cadenas"]))
        .withColumn("personal_min",
                    F.regexp_extract("per_ocu", r"^(\d+)", 1).cast("int"))
        .withColumn("latitud", F.col("latitud").cast("double"))
        .withColumn("longitud", F.col("longitud").cast("double"))
        .withColumn("fecha_corte", F.to_date(fechas[F.col("version")]))
    )

    sin_llave = df.filter(F.col("id").isNull() | F.col("cve_mun5").isNull())
    metricas["filas_sin_llave"] = sin_llave.count()
    df = df.filter(F.col("id").isNotNull() & F.col("cve_mun5").isNotNull())

    df_unico = df.dropDuplicates(["version", "id"])
    metricas["filas_duplicadas"] = (metricas["filas_interim"] - metricas["filas_sin_llave"]
                                    - df_unico.count())

    df_final = df_unico.select(
        "version", "fecha_corte", "id", "nom_estab", "raz_social", "codigo_act",
        "nombre_act", "categoria", "cadena", "per_ocu", "personal_min",
        "cve_ent", "entidad", "cve_mun", "municipio", "cve_mun5",
        "latitud", "longitud", "fecha_alta", "archivo",
    )
    metricas["filas_processed"] = df_final.count()
    return df_final, metricas


def ruta_csv_staging(interim):
    """Carpeta donde dejamos los CSV extraidos (texto sin modificar)."""
    return Path(interim) / "_csv_denue"
