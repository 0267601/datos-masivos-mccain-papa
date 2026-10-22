# Datos Masivos 2026 — McCain: ¿dónde invertir para vender más papas a la francesa en México?

Entregable 2 — Pipeline reproducible: adquisición, procesamiento, validación y desempeño.
Corre desde la terminal y en **Databricks**, con el data lake en Unity Catalog.

## Integrantes

Proyecto en parejas.

| Nombre | Matrícula |
|---|---|
| Diego Zamora González | 0267601 |
| Diego Pérez Palafox | 0269250 |

## Descripción

McCain opera en México desde 1995 y su producto central es la papa procesada
(papa a la francesa congelada). Esa papa requiere variedad industrial —tipo
Russet o Alpha— con alta materia seca y bajos azúcares reductores, no papa de
mesa genérica.

**No existe una visión pública de dónde se vende papa industrial en México.** Las
estadísticas agrícolas reportan cuánto se produce, pero no por qué canal se
comercializa. Cruzamos la producción del SIAP con las cotizaciones mayoristas del
SNIIM para encontrar los estados cuya papa **no pasa por el mercado spot**.

### Pregunta principal

> ¿Qué estados mexicanos venden su papa fuera del mercado mayorista público,
> y cuáles de ellos están creciendo lo suficiente para sustituir importación
> de papa industrial?

**Usuario:** equipo de abasto agrícola y desarrollo de proveedores de una procesadora
de papa (caso McCain México). **Decisión que habilita:** a qué estados dirigir contratos
agrícolas y en cuáles no vale la pena invertir.

### Pregunta reenfocada: dónde invertir

El E1 confirmó que la papa industrial es importada: **McCain no compra papa mexicana**,
importa papa congelada y la vende a restaurantes. Por eso movimos la pregunta del lado de
la oferta al de la demanda:

> Si McCain tuviera X millones para crecer en México, ¿en qué zonas metropolitanas
> los pondría?

Construimos un **índice de inversión** para las 92 zonas metropolitanas de CONAPO con el
DENUE de INEGI (2019 contra 2026): A. crecimiento de restaurantes donde se venden papas,
B. aperturas de cadenas (McDonald's, KFC, Carl's Jr.…), turismo y tamaño de mercado, con el
mismo peso. La pieza C, competencia, sale de UN Comtrade: de qué país vienen las papas
congeladas importadas y a qué precio por kilo. Método, fuentes y resultados en
[`docs/indice_inversion.md`](docs/indice_inversion.md).

## Arquitectura

```
SIAP (CSV por año)          SNIIM (HTML por variedad y mes)
        │                               │
        └──── src/ingestion/download_data ────┘   requests, caché, manifiesto
                        │
                  data/raw/                       copia cruda, sin modificar
                        │
              src/processing/build_dataset        PySpark
                        │
                  data/interim/                   esquema unificado, texto, Parquet
                        │
                  data/processed/                 tipado y limpio, Parquet particionado
                        │                           siap_produccion/   anio=
                        │                           sniim_precios/     anio=/mes=
                        │                           canalizacion/
                        │
              src/validation/validate_dataset     36 reglas → reports/tables/
                        │
                Dataset preparado
```

Las capas `raw/`, `interim/` y `processed/` viven en `data/` cuando corremos en local, y en el
Volume `workspace.mccain_papa.datalake` cuando corremos en Databricks (ver abajo).

| Fuente | Origen | Obtención | Periodo | Formato crudo |
|---|---|---|---|---|
| SIAP — Cierre de la Producción Agrícola | SADER | Descarga automatizada | 2013–2025, anual | CSV latin-1, todos los cultivos |
| SNIIM — Precios de Mercados Nacionales | Secretaría de Economía | Scraping con requests + BeautifulSoup | 2025, diario | HTML, una página por variedad y mes |

Para el índice de inversión sumamos tres fuentes:

| Fuente | Origen | Obtención | Periodo | Formato crudo |
|---|---|---|---|---|
| DENUE — sector 72 (restaurantes y hoteles) | INEGI | Descarga masiva de 7 zip | feb 2015, nov 2019, nov 2022, may 2026 | CSV dentro de zip; UTF-8 en 2015, latin-1 después |
| Las metrópolis de México 2020 | CONAPO | 2 CSV de datos.gob.mx | 2020 | CSV UTF-8 |
| Importaciones de papa congelada (HS 200410) | UN Comtrade | API pública, un mes por consulta | 2015–2025, mensual | JSON |

Ninguna fuente pide credenciales.

## Requisitos

- Python 3.10 o superior
- Java 17 o superior (lo necesita Spark). Verificar con `java -version`.
  En macOS: `brew install openjdk@17` o, con Anaconda, `conda install -c conda-forge openjdk=17`
  y `export JAVA_HOME="$CONDA_PREFIX/lib/jvm"`. En Colab ya viene instalado.
- `pyarrow` 19 o superior: versiones anteriores no leen los Parquet que escribe
  Spark 4 (`Repetition level histogram size mismatch`). `requirements.txt` ya lo pide.

## Instalación

```bash
git clone https://github.com/0267601/datos-masivos-mccain-papa.git
cd datos-masivos-mccain-papa
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Obtención de datos

Los datos crudos no se suben al repositorio. Hay dos formas de obtenerlos:

```bash
# Opción A — descarga real de las fuentes (unos 5 minutos)
python -m src.ingestion.download_data

# Opción B — sin internet: reconstruye data/raw/ desde las copias del E1
python -m src.ingestion.download_data --muestra
```

Los archivos se guardan en `data/raw/siap/` y `data/raw/sniim/`, junto con
`data/raw/manifiesto.json` (url, parámetros, bytes, sha256 y estado de cada archivo).
Si un archivo ya existe no se vuelve a descargar; `--forzar` lo ignora. Si una
partición falla, se registra en `logs/pipeline.log` y la corrida sigue.

## Ejecución

Desde la raíz del repositorio, en este orden:

```bash
python -m src.ingestion.download_data        # 1. adquisición    → data/raw/
python -m src.processing.build_dataset       # 2. procesamiento  → data/interim/, data/processed/
python -m src.validation.validate_dataset    # 3. validación     → reports/tables/validacion.csv
python -m src.processing.benchmark           # 4. desempeño      → reports/tables/desempeno.csv
pytest                                       # pruebas
```

Índice de inversión (ver [`docs/indice_inversion.md`](docs/indice_inversion.md)):

```bash
python -m src.ingestion.download_data --fuente indice    # DENUE, CONAPO, Comtrade (~15 min)
python -m src.processing.build_dataset --fuente indice   # → processed/denue_establecimientos, metropolis, importaciones
python -m src.analysis.indice_inversion                  # → reports/tables/indice_inversion.csv y 3 gráficas
python -m src.validation.validate_dataset                # valida lo que exista: E1 e índice
```

`--fuente e1` hace solo SIAP y SNIIM; sin `--fuente` se hace todo.

Cada comando regresa código de salida 1 si algo falla, así que se pueden encadenar con `&&`.

`--muestra` no sobrescribe una descarga real: si `data/raw/` ya tiene archivos
descargados, termina con código 1 sin tocar nada (`--muestra --forzar` la reemplaza).

Para ver el índice de canalización en Excel o Numbers:

```bash
python -c "import pandas as pd; pd.read_parquet('data/processed/canalizacion').sort_values(['anio','ranking']).to_csv('reports/tables/canalizacion.csv', index=False)"
```

Los años, variedades, pausas y umbrales de validación están en
`config/config.example.yaml`. Para cambiarlos localmente, copiarlo a `config/config.yaml`
(ignorado por git).

`src/siap_extractor.py` y `src/sniim_extractor.py` solo reexportan los de
`src/ingestion/`, para que `notebooks/entregable_1.ipynb` siga corriendo sin cambios.

El notebook `notebooks/02_cleaning.ipynb` corre el mismo pipeline paso a paso y muestra
los planes de Spark (partition pruning, predicate pushdown, broadcast y shuffle).

## Ejecución en Databricks (data lake)

El mismo pipeline corre en Databricks sin copiar código: abrimos este repositorio como
**Git folder** y corremos `notebooks/03_databricks.ipynb` en cómputo serverless. Las capas
quedan en un Volume de Unity Catalog y la capa final, como tablas Delta:

```
workspace.mccain_papa
├── datalake (Volume)   raw/ · interim/ · processed/ · reports/
├── siap_produccion     tabla Delta
├── sniim_precios       tabla Delta
└── canalizacion        tabla Delta
```

Pasos:

1. **Workspace → Create → Git folder** con la URL
   `https://github.com/0267601/datos-masivos-mccain-papa.git`.
2. Abrir `notebooks/03_databricks.ipynb`, **Connect → Serverless** y **Run all**.
3. Para el índice de inversión: `notebooks/04_indice_databricks.ipynb`, **Run all**. Descarga
   el DENUE al Volume, calcula el índice, muestra las tres gráficas y registra las tablas
   `indice_inversion`, `importaciones_anuales`, `serie_metropolis` y demás.

El paso a paso, qué hacer si Databricks no alcanza los portales del gobierno y cómo consultar
las tablas está en [`docs/databricks.md`](docs/databricks.md).

## Resultados principales

**Pipeline.** Con la descarga real (21 de octubre de 2026):

| Capa | SIAP | SNIIM |
|---|---|---|
| raw | 13 CSV, 72.3 MB (todos los cultivos) | 72 HTML, 8.4 MB, 0 truncados |
| interim | 3,171 filas de papa | 11,010 cotizaciones |
| processed | 3,171 filas, 0 duplicados, 0 nulos en llaves | 11,010 filas, 0 duplicados, 0 nulos en llaves |

La descarga tarda unos 4 minutos (pausa de cortesía entre peticiones); procesar y
validar, unos 20 segundos. Con `--muestra` (SNIIM solo enero–marzo, 2,683 filas) el
pipeline completo corre en unos 25 segundos.

**Validación.** 36 reglas: 34 OK, 2 AVISO, 0 FALLA. Los avisos son una cotización de
$0.43/kg (error de captura del SNIIM) y dos estados que cotizan sin producir (Nayarit y
Yucatán). Ver [`docs/validation.md`](docs/validation.md).

**Índice de canalización 2025.**

| Estado | % producción nacional | % cotizaciones mayoristas | Canalización |
|---|---|---|---|
| Sonora | 30.8 | 2.6 | **+28.1** |
| Sinaloa | 27.1 | 24.0 | +3.1 |
| Nuevo León | 5.0 | 2.3 | +2.7 |
| … | | | |
| Coahuila | 0.5 | 11.2 | −10.7 |
| Puebla | 7.1 | 21.7 | −14.6 |

Positivo: el estado produce más de lo que aparece en el mercado spot (vende por
contrato). Negativo: aparece más de lo que produce (redistribuye).

**Desempeño.** Con un millón de filas, pandas lee Parquet 3.7 veces más rápido que CSV y
el archivo pesa 11 veces menos; a nuestro volumen pandas le gana a Spark por el costo fijo
de arranque de Spark. Ver [`docs/desempeno.md`](docs/desempeno.md).

**Hallazgo (se mantiene del E1).** Sonora produce cerca del 31% de la papa del país y solo
aparece en el 2.6% de las cotizaciones mayoristas: su papa sale por contrato. La Russet, la variedad
industrial, es 99.6% importada. Ver [`docs/hallazgos.md`](docs/hallazgos.md).

## Estructura del repositorio

```
datos-masivos-mccain-papa/
├── README.md
├── CONTRIBUTING.md
├── requirements.txt
├── pyproject.toml
├── config/
│   └── config.example.yaml        años, variedades, pausas, umbrales
├── data/
│   ├── raw/                       copia cruda (no se sube)
│   ├── interim/                   esquema unificado (no se sube)
│   ├── processed/                 dataset preparado (no se sube)
│   ├── sample/                    muestras pequeñas y su README
│   ├── siap_papa_2013_2025.csv    copia del E1 (base de --muestra)
│   └── sniim_papa_2025.csv        copia del E1 (base de --muestra)
├── docs/
│   ├── project_scope.md
│   ├── data_dictionary.md
│   ├── architecture.md
│   ├── decisions.md
│   ├── validation.md
│   ├── desempeno.md
│   ├── databricks.md              cómo correrlo en Databricks
│   ├── hallazgos.md
│   └── indice_inversion.md        pregunta reenfocada: dónde invertir
├── notebooks/
│   ├── entregable_1.ipynb         entregable 1: extractores paso a paso
│   ├── 02_cleaning.ipynb          entregable 2: pipeline y planes de Spark
│   ├── 03_databricks.ipynb        entregable 2 en Databricks: data lake y tablas Delta
│   └── 04_indice_databricks.ipynb índice de inversión en Databricks, con gráficas
├── src/
│   ├── ingestion/                 siap, sniim, denue y comtrade extractors, download_data, muestra
│   ├── processing/                siap, sniim, canalizacion, denue, metropolis, comtrade, build_dataset, benchmark
│   ├── validation/                reglas, reglas_indice, validate_dataset
│   ├── analysis/                  indice_inversion, graficas
│   └── utils/                     rutas, config, registro, spark
├── tests/                         test_ingestion, test_processing, test_validation, test_indice
└── reports/
    ├── figures/
    └── tables/
```

## Limitaciones

- **SNIIM reporta el origen declarado en el mercado, no el estado productor.**
  Coahuila, Nayarit y Yucatán aparecen como origen con poca o nula producción en SIAP.
- **SIAP no distingue variedad.** El cruce con SNIIM lo compensa parcialmente.
- **SIAP cambia encabezados entre años y nombres de estado** ("Ciudad de México / DF").
  Los normalizamos en `src/processing/siap.py`.
- **SNIIM trunca a 1000 registros sin avisar.** Particionamos por mes y el manifiesto
  marca cada partición que toque el límite; la validación falla si alguna lo toca.
- **Los portales del gobierno se caen con frecuencia.** Por eso la caché, los reintentos
  con espera creciente y la opción `--muestra`.
- **El volumen actual no exige Spark.** Lo usamos para que el pipeline escale sin
  reescribirse; la comparación de desempeño lo documenta.
