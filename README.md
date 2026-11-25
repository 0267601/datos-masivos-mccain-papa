# Datos Masivos 2026 — McCain: ¿dónde invertir para vender más papas a la francesa en México?

Examen final — sistema integrado: adquisición, procesamiento, validación, análisis, modelo
y comunicación. Corre con un comando (`python main.py`) y en **Databricks**, con el data lake
en Unity Catalog.

**Respuesta corta.** Un modelo XGBoost estima US$32 M de mercado nuevo de papa congelada en
las 92 metrópolis al 2030. En dólares, ese crecimiento está en Ciudad de México, Guadalajara y
Monterrey (29%); donde la oportunidad de hoy (índice de inversión) y el crecimiento esperado
coinciden es en ciudades medianas: **Querétaro, La Laguna, Mérida, Saltillo y
Puebla-Tlaxcala**. Las turísticas que encabezan el índice (Los Cabos, Acapulco) ya tienen
muchos restaurantes por habitante y el modelo espera que se frenen. En un **escenario simulado**
(supuestos, no datos de McCain), McCain dejaría US$25.1 M de margen de contribución en 2030, y
con **US$10 M** para crecer convendría entrar a Monterrey, Guadalajara y Ciudad de México
(+US$1.5 M al año, 6.6 años para recuperar). Todo se explora en el heatmap interactivo
`reports/figures/mapa_potencial.html` (entregable aparte). Reporte completo en
[`docs/final_report.md`](docs/final_report.md).

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

## Pregunta principal

> Si McCain tuviera X millones para crecer en México, ¿en qué zonas metropolitanas
> los pondría, y cuánto crecería ahí el mercado de papas a la francesa?

**Usuario:** equipo comercial, de expansión y de planeación de demanda de McCain México.
**Decisión que habilita:** en qué ciudades abrir distribución, fuerza de ventas o promociones
primero.

### Antecedente: la pregunta de los entregables 1 y 2

> ¿Qué estados mexicanos venden su papa fuera del mercado mayorista público,
> y cuáles de ellos están creciendo lo suficiente para sustituir importación
> de papa industrial?

Esa pregunta, del lado del abasto, se respondió con el cruce SIAP × SNIIM y su pipeline sigue
en el repositorio (`--fuente e1`).

### Por qué la reenfocamos

El E1 confirmó que la papa industrial es importada: **McCain no compra papa mexicana**,
importa papa congelada y la vende a restaurantes. Por eso movimos la pregunta del lado de
la oferta al de la demanda:

> Si McCain tuviera X millones para crecer en México, ¿en qué zonas metropolitanas
> los pondría?

Construimos un **índice de inversión** para las 92 zonas metropolitanas de CONAPO con el
DENUE de INEGI (2019 contra 2026): A. crecimiento de restaurantes donde se venden papas (comida rápida y a la carta),
B. aperturas de cadenas (McDonald's, KFC, Carl's Jr.…), turismo y tamaño de mercado, con el
mismo peso. La pieza C, competencia, sale de UN Comtrade: de qué país vienen las papas
congeladas importadas y a qué precio por kilo. Método, fuentes y resultados en
[`docs/indice_inversion.md`](docs/indice_inversion.md).

Encima del índice, un **modelo XGBoost** aprende cómo crecieron los restaurantes de cada
municipio entre las cuatro versiones del DENUE y proyecta 2026 → 2030. El **potencial en US$**
de cada metrópoli es restaurantes proyectados × kg de papa congelada importada por
restaurante × US$/kg. Es tamaño de mercado, no utilidad: no usamos ventas ni costos de McCain.

## Arquitectura

```
Fuentes abiertas ─► download_data ─► raw/ ─► build_dataset (PySpark) ─► interim/ ─► processed/
SIAP · SNIIM         caché, reintentos,       Bronze: archivos  Silver: tipos, llaves,  (Silver)
DENUE · CONAPO       manifiesto sha256                          rechazos, conciliación
UN Comtrade                                                                │
                                              validate_dataset ◄───────────┘  FALLA ⇒ se detiene
                                                     │
                                  indice_inversion ──┴── modelo_potencial (XGBoost)
                                     (Gold)                  (Gold)
                                        │                       │
                       reports/: tablas, gráficas, mapa_potencial.html · tablas Delta en Unity Catalog

main.py ─► src/pipeline.py: leer_configuracion → ejecutar_pipeline → ejecutar_etapa (registro JSON)
```

Detalle de cada capa, el modelo, la orquestación y el flujo de colaboración en
[`docs/architecture.md`](docs/architecture.md).

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

Con `--muestra` también se copian la muestra real del índice (`data/sample/indice/`: un
extracto del DENUE de tres metrópolis y los 132 meses de Comtrade) y CONAPO
(`data/sample/conapo/`), así `python main.py --muestra` corre todo sin internet.

Los archivos se guardan en `data/raw/siap/` y `data/raw/sniim/`, junto con
`data/raw/manifiesto.json` (url, parámetros, bytes, sha256 y estado de cada archivo).
Si un archivo ya existe no se vuelve a descargar; `--forzar` lo ignora. Si una
partición falla, se registra en `logs/pipeline.log` y la corrida sigue.

## Ejecución

**Todo con un comando** (adquisición → Silver → validación → Gold → modelo):

```bash
python main.py                    # descarga lo que falte y corre todo
python main.py --sin-descarga     # usa lo que ya está en data/raw/
python main.py --muestra          # sin internet: muestra real del repositorio
python main.py --fuente indice    # solo el índice y el modelo
```

Cada etapa imprime una línea JSON con su estado y duración; si una falla, las siguientes no
corren y el comando termina con código 1. El historial queda en
`reports/tables/registro_pipeline.json`.

Resultados en: `reports/tables/` (índice, potencial, métricas del modelo, validación) y
`reports/figures/` (gráficas PNG y `mapa_potencial.html`, que se abre en cualquier navegador).

**Por etapas**, desde la raíz del repositorio, en este orden:

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
python -m src.validation.validate_dataset                # valida lo que exista: E1 e índice
python -m src.analysis.run_analysis                      # índice + modelo XGBoost + rentabilidad + heatmap
```

`run_analysis` corre `src.analysis.indice_inversion` (índice y 3 gráficas) y
`src.analysis.modelo_potencial` (modelo, potencial en US$, importancia de variables y
`mapa_potencial.html`) y `src.analysis.rentabilidad` (XGBoost por segmento, escenario de
rentabilidad y `reports/rentabilidad_escenario.xlsx`).

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

**Evidencia de la corrida:** [`notebooks/04_indice_databricks_ejecutado.ipynb`](notebooks/04_indice_databricks_ejecutado.ipynb)
guarda las salidas reales del workspace (tablas, validación, las tres gráficas y el plan de Spark con
partition pruning). GitHub lo muestra sin instalar nada.

Así quedó el data lake en Unity Catalog:

```
workspace.mccain_papa
├── datalake (Volume)
│   ├── raw/          siap/ · sniim/ · denue/<versión>/ · metropolis/ · comtrade/ · manifiesto.json
│   ├── interim/      siap/ · sniim/ · denue/ (particionado por versión)
│   ├── processed/    siap_produccion/ · sniim_precios/ · canalizacion/
│   │                 denue_establecimientos/ · metropolis/ · importaciones/ · indice_inversion/ …
│   └── reports/      tables/validacion.csv · tables/indice_inversion.csv · figures/*.png
└── tablas Delta
    ├── siap_produccion            3,171 filas
    ├── sniim_precios             11,010
    ├── canalizacion           por estado y año
    ├── denue_establecimientos  2,774,429
    ├── indice_inversion              92
    ├── serie_metropolis             368
    ├── cadenas_metropolis         1,620
    ├── metropolis                    92
    ├── importaciones                527
    └── importaciones_anuales         51
```

Para reproducirlo en otro workspace basta abrir el repo como Git folder y dar **Run all** en
`03_databricks.ipynb` (entregable 2) y `04_indice_databricks.ipynb` (índice). Si datos.gob.mx bloquea
la descarga de CONAPO, el pipeline usa la copia de `data/sample/conapo/`.

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
4. Para el modelo: `notebooks/05_modelo_potencial_databricks.ipynb`, **Run all**. Corre el
   pipeline completo con `ejecutar_pipeline`, registra `potencial_metropolis`,
   `panel_municipios` y `denue_rechazos` y arma `reports/entrega_final.zip`. Se puede
   programar como **Job**: sus widgets son los parámetros.

**Entregable principal aparte: heatmap interactivo.** `reports/figures/mapa_potencial.html` no
se muestra en el notebook; es un archivo propio que se abre en cualquier navegador: mapa de
México con todas las metrópolis y un selector de tres vistas:

- **Mercado:** potencial al 2030, índice y crecimiento; al hacer clic en una metrópoli, sus
  números e insights.
- **Rentabilidad\*:** margen de contribución por ciudad y segmento, con la matriz de supuestos
  editable (precio, descuentos, costos, participación).
- **Inversión\*:** "si McCain tuviera X millones": reparto con rendimientos decrecientes, costo
  de entrada por ciudad (fijo + % de su mercado), años de recuperación y la participación actual
  de cada ciudad editable.

\* Escenario simulado, no datos de McCain. Todo se recalcula en la página al cambiar un
supuesto. Lo genera el pipeline y viaja también en la raíz de `entrega_final.zip`.

**Entregable aparte: escenario de rentabilidad en Excel.** `reports/rentabilidad_escenario.xlsx`
reparte el mercado en cuatro segmentos (cadenas, comida rápida independiente, a la carta
independiente y hoteles), proyecta sus locales a 2030 con un XGBoost por segmento y calcula el
**margen de contribución** de McCain con una matriz de supuestos editable (precio de lista,
descuento, costo de servir y participación). Es un **escenario simulado**, no datos de McCain:
al cambiar una celda azul de la hoja *Supuestos*, todo el libro se recalcula. El reparto de
X millones vive solo en el heatmap. Detalle en [`docs/final_report.md`](docs/final_report.md).

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

**Índice de inversión (pregunta reenfocada).** Top 5 de 92 zonas metropolitanas: Los Cabos
(82.7), Acapulco (73.4), Guanajuato (73.1), Tecomán (72.0) y Tapachula (71.1). La oportunidad
está en ciudades medianas turísticas o industriales; las tres grandes no entran al top 15. La
papa congelada importada se triplicó desde 2019 (US$455 M en 2025) y Canadá pasó de 10% a 39%
del valor importado. Ver [`docs/indice_inversion.md`](docs/indice_inversion.md).

**Rentabilidad e inversión (escenario simulado, no datos de McCain).** Margen de contribución de
US$23.0 M hoy a US$25.1 M en 2030 (ventas de US$140 M, margen de 18%); la comida rápida
independiente deja 56%. Con US$10 M: Monterrey, Guadalajara y Ciudad de México, +US$1.5 M al
año, 6.6 años para recuperar; pasados US$50 M el dinero rinde poco (el tramo de 50 a 100 millones
recupera en 14 años). Ver [`docs/final_report.md`](docs/final_report.md).

**Modelo de potencial** (Databricks, 23 de noviembre de 2026). Validación temporal: entrena
2015→2019 y 2019→2022 y prueba 2022→2026 en 359 municipios.

| Modelo | MAE municipal (pp) | Spearman entre metrópolis |
|---|---|---|
| **XGBoost** | **3.65** | **0.19** |
| Persistencia | 4.10 | 0.05 |
| Promedio | 3.96 | — |

Le gana a las dos referencias (11% menos error que repetir el crecimiento anterior), aunque
ordena poco a las metrópolis, así que lo usamos como escenario. Con 2,327 kg de papa
importada por restaurante al año a US$1.30/kg (Comtrade 2025), el mercado de las 92
metrópolis pasa de US$313 M a US$345 M en 2030. Top 5 en mercado nuevo: Ciudad de México
(US$5.3 M), Guadalajara (2.3), Monterrey (1.8), Puebla-Tlaxcala (1.3) y Tijuana (1.1). Lo que
más pesa es la densidad de restaurantes por habitante, en contra (saturación). Detalle en
[`docs/final_report.md`](docs/final_report.md).

**Pipeline completo** (`ejecutar_pipeline`, `reports/tables/registro_pipeline.json`): 2 min
26 s en Databricks serverless con caché; validación 70 reglas, 67 OK, 3 AVISO, 0 FALLA. Sin
internet, `python main.py --muestra` corre todo con la muestra real de `data/sample/` en
alrededor de 1 minuto.

## Notebooks y la guía del proyecto

| La guía pide | En este repositorio |
|---|---|
| `01_exploration.ipynb` | `entregable_1.ipynb` (exploración del E1: dimensiones, tipos, faltantes, duplicados) |
| `02_cleaning.ipynb` | `02_cleaning.ipynb` (pipeline paso a paso y planes de Spark) |
| `03_analysis.ipynb` | `04_indice_databricks.ipynb` (índice de inversión y competencia) |
| `04_results.ipynb` | `05_modelo_potencial_databricks.ipynb` (modelo y potencial; el heatmap es un HTML aparte) y `04_indice_databricks_ejecutado.ipynb` (salidas reales) |
| — | `03_databricks.ipynb` (pipeline del E2 en Databricks) |

## Estructura del repositorio

```
datos-masivos-mccain-papa/
├── README.md
├── CONTRIBUTING.md                cómo trabajamos: issues, ramas, PR, coautoría
├── main.py                        comando principal: corre todo el pipeline
├── requirements.txt
├── pyproject.toml
├── config/
│   └── config.example.yaml        años, variedades, pausas, umbrales
├── data/
│   ├── raw/                       copia cruda (no se sube)
│   ├── interim/                   esquema unificado (no se sube)
│   ├── processed/                 dataset preparado (no se sube)
│   ├── sample/                    muestras pequeñas: E1, conapo/ e indice/ (DENUE y Comtrade reales)
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
│   ├── indice_inversion.md        pregunta reenfocada: dónde invertir
│   ├── final_report.md            reporte final
│   ├── roadmap.md                 entregas, checkpoints y evidencia contra la guía
│   └── reflections/               reflexión individual de cada integrante
├── notebooks/
│   ├── entregable_1.ipynb         entregable 1: extractores paso a paso
│   ├── 02_cleaning.ipynb          entregable 2: pipeline y planes de Spark
│   ├── 03_databricks.ipynb        entregable 2 en Databricks: data lake y tablas Delta
│   ├── 04_indice_databricks.ipynb índice de inversión en Databricks, con gráficas
│   ├── 04_indice_databricks_ejecutado.ipynb   la corrida real, con sus salidas
│   └── 05_modelo_potencial_databricks.ipynb   pipeline completo y modelo
├── src/
│   ├── ingestion/                 siap, sniim, denue y comtrade extractors, download_data, muestra, muestra_indice
│   ├── processing/                siap, sniim, canalizacion, denue, metropolis, comtrade, build_dataset, benchmark
│   ├── validation/                reglas, reglas_indice, validate_dataset
│   ├── pipeline.py                leer_configuracion, ejecutar_etapa, ejecutar_pipeline
│   ├── analysis/                  indice_inversion, modelo_potencial, mapa_potencial, graficas, run_analysis
│   └── utils/                     rutas, config, registro, spark
├── tests/                         test_ingestion, test_processing, test_validation, test_indice, test_modelo
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
- **El DENUE cuenta locales, no ventas.** Un McDonald's y una fonda pesan igual en el
  crecimiento; por eso las cadenas tienen su propio componente.
- **El potencial en US$ es tamaño de mercado, no utilidad.** Supone que el consumo por
  restaurante y el precio por kilo se mantienen y que todo el mercado es papa importada.
- **El modelo aprende de tres periodos**, uno de ellos con la pandemia en medio. Lo
  comparamos contra referencias simples en un periodo que no vio y reportamos el resultado
  tal cual.
- **El volumen actual del E1 no exige Spark.** Lo usamos para que el pipeline escale sin
  reescribirse; la comparación de desempeño lo documenta.
