# Datos Masivos 2026 — McCain: ¿dónde se vende la papa industrial en México?

Entregable 2 — Pipeline reproducible: adquisición, procesamiento, validación y desempeño.

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

| Fuente | Origen | Obtención | Periodo | Formato crudo |
|---|---|---|---|---|
| SIAP — Cierre de la Producción Agrícola | SADER | Descarga automatizada | 2013–2025, anual | CSV latin-1, todos los cultivos |
| SNIIM — Precios de Mercados Nacionales | Secretaría de Economía | Scraping con requests + BeautifulSoup | 2025, diario | HTML, una página por variedad y mes |

Ninguna fuente pide credenciales.

## Requisitos

- Python 3.10 o superior
- Java 17 o superior (lo necesita Spark). Verificar con `java -version`.
  En macOS: `brew install openjdk@17`. En Colab ya viene instalado.

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

Cada comando regresa código de salida 1 si algo falla, así que se pueden encadenar con `&&`.

Los años, variedades, pausas y umbrales de validación están en
`config/config.example.yaml`. Para cambiarlos localmente, copiarlo a `config/config.yaml`
(ignorado por git).

`src/siap_extractor.py` y `src/sniim_extractor.py` solo reexportan los de
`src/ingestion/`, para que `notebooks/entregable_1.ipynb` siga corriendo sin cambios.

El notebook `notebooks/02_cleaning.ipynb` corre el mismo pipeline paso a paso y muestra
los planes de Spark (partition pruning, predicate pushdown, broadcast y shuffle).

## Resultados principales

**Pipeline.** Con la muestra, el pipeline completo tarda unos 25 segundos:

| Capa | SIAP | SNIIM |
|---|---|---|
| raw | 13 CSV | 15 HTML |
| interim | 3,171 filas | 2,683 filas |
| processed | 3,171 filas, 0 duplicados, 0 nulos en llaves | 2,683 filas, 0 duplicados, 0 nulos en llaves |

**Validación.** 36 reglas: 34 OK, 2 AVISO, 0 FALLA. Ver [`docs/validation.md`](docs/validation.md).

**Desempeño.** Con un millón de filas, pandas lee Parquet 3.7 veces más rápido que CSV y
el archivo pesa 11 veces menos; a nuestro volumen pandas le gana a Spark por el costo fijo
de arranque de Spark. Ver [`docs/desempeno.md`](docs/desempeno.md).

**Hallazgo (se mantiene del E1).** Sonora produce cerca del 31% de la papa del país y casi
no aparece en el mercado mayorista: su papa sale por contrato. La Russet, la variedad
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
│   └── hallazgos.md
├── notebooks/
│   ├── entregable_1.ipynb         entregable 1: extractores paso a paso
│   └── 02_cleaning.ipynb          entregable 2: pipeline y planes de Spark
├── src/
│   ├── ingestion/                 siap_extractor, sniim_extractor, download_data, muestra
│   ├── processing/                siap, sniim, canalizacion, build_dataset, benchmark
│   ├── validation/                reglas, validate_dataset
│   ├── analysis/                  (examen final)
│   └── utils/                     rutas, config, registro, spark
├── tests/                         test_ingestion, test_processing, test_validation
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
