# Arquitectura

## Arquitectura real (entregable 2)

El código vive en GitHub y corre igual en dos lugares; solo cambia dónde guardamos las capas:

| Dónde corre | Cómo se lanza | Dónde viven raw / interim / processed |
|---|---|---|
| Local | `python -m src.…` desde la terminal | `data/` dentro del repositorio |
| Databricks | `notebooks/03_databricks.ipynb` desde el Git folder del repo | Volume `workspace.mccain_papa.datalake` (el data lake) |


```
SIAP: CSV por año (latin-1)          SNIIM: HTML por variedad y mes
            │                                      │
            └────────── src/ingestion/download_data ──────────┘
                         requests · caché · reintentos · manifiesto
                                      │
                                 raw/   ← copia cruda, sin modificar
                                      │
                        src/processing/build_dataset  (PySpark)
                                      │
               ┌──────────────────────┴───────────────────────┐
     SIAP: leer año por año,                       SNIIM: read.text(wholetext) +
     unificar encabezados, unionByName,            UDF(BeautifulSoup) en paralelo
     filtrar papa
               └──────────────────────┬───────────────────────┘
                                      │
                               interim/   ← esquema unificado, todo texto (Parquet)
                                      │
                 tipos · nombres · estados · duplicados · nulos · variables
                                      │
                              processed/  ← Parquet
                     siap_produccion/  (anio=)
                     sniim_precios/    (anio=/mes=)
                     canalizacion/     (cruce SIAP × SNIIM, broadcast + window)
                                      │
                       src/validation/validate_dataset
                         36 reglas · OK / AVISO / FALLA
                                      │
                              Dataset preparado
                                      │
                ┌─────────────────────┴─────────────────────┐
     src/processing/benchmark (local)        Tablas Delta en Databricks
         comparación de desempeño            workspace.mccain_papa.{siap_produccion,
                                                 sniim_precios, canalizacion}
```

En Databricks el flujo completo queda así:

```
GitHub (0267601/datos-masivos-mccain-papa)
        │  Git folder
        ▼
Workspace de Databricks ── notebooks/03_databricks.ipynb (serverless)
        │  llama a src/ingestion, src/processing, src/validation
        ▼
Unity Catalog
└── workspace
    └── mccain_papa                       esquema del proyecto
        ├── datalake  (Volume)            el data lake
        │   ├── raw/        siap/, sniim/, manifiesto.json
        │   ├── interim/    siap_papa/, sniim_precios/
        │   ├── processed/  siap_produccion/, sniim_precios/, canalizacion/
        │   └── reports/    tables/validacion.csv
        ├── siap_produccion   (tabla Delta)
        ├── sniim_precios     (tabla Delta)
        └── canalizacion      (tabla Delta)
```

## Dónde se almacenan los datos

| Capa | Ruta local | Ruta en Databricks | Formato | ¿Se sube a GitHub? |
|---|---|---|---|---|
| raw | `data/raw/` | `/Volumes/workspace/mccain_papa/datalake/raw/` | CSV y HTML tal como llegan, más `manifiesto.json` | No |
| interim | `data/interim/` | `…/datalake/interim/` | Parquet, todo como texto | No |
| processed | `data/processed/` | `…/datalake/processed/` y tablas Delta `workspace.mccain_papa.*` | Parquet tipado y particionado | No |
| sample | `data/sample/` y copias del E1 en `data/` | CSV pequeños; `--muestra` reconstruye raw a partir de las copias | Sí |
| resultados | `reports/tables/`, `reports/figures/` | `…/datalake/reports/` | CSV, Markdown, SVG | Sí (los de local) |

## Cómo se obtienen

| Fuente | Método | Partición de la descarga | Protección |
|---|---|---|---|
| SIAP | `requests.get` al portal de datos abiertos | Un CSV por año | Caché por archivo, escritura atómica |
| SNIIM | `requests.get` a la consulta de precios | Una página por variedad y mes (evita el tope de 1000 registros) | Caché, reintentos con espera creciente ante 503, pausa entre peticiones, bandera de truncado |
| DENUE | `requests.get` por bloques de 1 MB a la descarga masiva de INEGI | Un zip por versión y parte (7) | Caché, rechazo del "404 suave" (HTML con código 200) revisando la firma `PK` del zip |
| CONAPO | `requests.get` a datos.gob.mx | 2 CSV | Caché, rechazo de respuestas HTML |
| UN Comtrade | API pública, `requests.get` | Un mes por consulta (la API no acepta más) | Caché por mes, reintentos ante 429 y 5xx, pausa entre consultas |

## Qué transformaciones hacemos

| Etapa | SIAP | SNIIM |
|---|---|---|
| raw → interim | Encabezados sin espacios; `Nomcultivo Sin Um` → `Nomcultivo`; `Precio` → `Preciomediorural`; columnas faltantes como nulo; unión por nombre; solo papa | Parseo de `table#tblResultados` / `td.Datos2`; variedad desde el ProductoId; archivo de origen |
| interim → processed | Números como double (sin comas de miles); snake_case con unidad; capital unificada; llave de estado sin acentos; sin nulos en llaves; sin duplicados; `pct_siniestrada`, `valor_por_ton` | Fecha como date; precios como double; estado y mercado destino; `es_importacion`; estado de origen estandarizado; sin nulos en llaves; sin duplicados; `rango_precio` |
| cruce | Producción por estado-año | Cotizaciones nacionales por estado-año → índice de canalización con window functions |

## Qué herramientas usamos

| Herramienta | Para qué |
|---|---|
| requests + BeautifulSoup | Descarga y parseo del SNIIM (igual que en el E1) |
| PySpark | Todo el procesamiento, la validación y el cruce |
| Databricks (serverless) + Unity Catalog | Correr el pipeline en la nube; Volume como data lake y tablas Delta para consultar con SQL |
| Parquet | Formato de interim y processed |
| PyYAML | Configuración sin tocar código |
| matplotlib | Las tres gráficas del índice de inversión (sobre tablas ya agregadas) |
| logging | `logs/pipeline.log` con cada descarga y cada error (en Databricks, en el disco del driver y en la salida del notebook) |
| pytest | 36 pruebas sin internet |
| pandas | Solo al final, para tablas chicas y la comparación de desempeño |

## Qué podría dejar de funcionar al crecer el volumen

| Punto | Por qué | Qué haríamos |
|---|---|---|
| Descarga del SNIIM | Es secuencial con pausa de 1 s: 6 variedades × 12 meses × N años | Paralelizar con pocos hilos sin saturar el portal |
| Tope de 1000 registros | Un mes con muchos mercados podría tocarlo | Partir por quincena; el manifiesto ya avisa cuál |
| CSV crudo del SIAP | Trae todos los cultivos (~6 MB por año) y Spark no puede empujar filtros a un CSV | Convertir raw a Parquet una vez y filtrar ahí |
| `toPandas()` | Lleva todo al driver | Solo lo usamos en tablas agregadas |
| Full outer join del cruce | Genera shuffle | Hoy es diminuto; si creciera, particionar por año antes |

## Índice de inversión

```
DENUE (7 zip)        CONAPO (2 CSV)        UN Comtrade (132 JSON)
      │                    │                       │
      └─────────── download_data --fuente indice ──┘
                           │
                     data/raw/denue/<versión>/ · metropolis/ · comtrade/
                           │
              build_dataset --fuente indice          PySpark
                           │   zip → CSV extraído → lectura por versión con su
                           │   codificación → unionByName (schema drift 2015 vs 2019+)
                     data/interim/denue/              version=
                     data/processed/denue_establecimientos/   version=
                     data/processed/metropolis/, importaciones/
                           │
              analysis.indice_inversion               broadcast join DENUE × municipios,
                           │                          percent_rank (window)
                     data/processed/indice_inversion/, serie_metropolis/
                     reports/tables/indice_inversion.csv
                     reports/figures/*.png
```

## Arquitectura final (examen final)

```
 FUENTES ABIERTAS                         ADQUISICIÓN                 CAPAS (Volume en Databricks / data/ en local)
 ─────────────────                        ───────────                 ──────────────────────────────────────────────
 SIAP · SNIIM (E1)                                                     raw/        archivos tal como llegan (zip, CSV, HTML, JSON)
 DENUE 2015·2019·2022·2026  ──►  download_data  (requests, caché,  ──►      │      manifiesto con sha256 por archivo
 CONAPO 2020                     reintentos, firma PK, respaldo)           ▼
 UN Comtrade HS 200410                                                 interim/    BRONZE tabular: un esquema por fuente, todo texto
                                                                           │       (unionByName contra el schema drift)
                                  build_dataset (PySpark)                  ▼
                                  construir_silver: tipos, llaves,     processed/  SILVER: denue_establecimientos, siap, sniim,
                                  motivos_rechazo, huella, conciliación            metropolis, importaciones (+ denue_rechazos)
                                                                           │
                                  validate_dataset (69+ reglas)  ── FALLA → se detiene, no hay Gold
                                                                           ▼
                                  indice_inversion (broadcast, window) GOLD: indice_inversion, serie_metropolis, cadenas_metropolis
                                  modelo_potencial (XGBoost)            GOLD: panel_municipios, potencial_metropolis
                                  rentabilidad (XGBoost por segmento    GOLD: rentabilidad_segmentos, rentabilidad_metropolis
                                  + supuestos del escenario)                  (ESCENARIO SIMULADO)
                                                                           │
                                                                           ▼
                                                                       reports/    tablas CSV, gráficas PNG, mapa_potencial.html,
                                                                                   rentabilidad_escenario.xlsx
                                                                       Unity Catalog: tablas Delta en workspace.mccain_papa
```

### Las capas, con el vocabulario de la clase

| Capa de la clase | Pregunta | Nuestra carpeta | Una fila es |
|---|---|---|---|
| Bronze | ¿Qué recibimos? | `raw/` (archivos) e `interim/` (tabular, texto) | Un registro tal como lo publicó la fuente |
| Silver | ¿Qué podemos usar y bajo qué reglas? | `processed/` | Un establecimiento, cotización o mes-país validado y sin duplicados |
| Gold | ¿Qué necesita el usuario? | `processed/` (tablas de análisis) y `reports/` | Una metrópoli (índice, potencial) o un municipio-versión (panel) |

Conciliación del DENUE (`construir_silver`): **interim = silver + rechazos + duplicados**. Los
rechazos se guardan con su `motivos_rechazo` (`id_invalido`, `municipio_invalido`,
`version_sin_fecha`) y los duplicados se resuelven con una huella sha256 de las columnas, para
que la elección no dependa del orden de lectura.

### Orquestación

`python main.py` (o el notebook 05 en Databricks) llama a `ejecutar_pipeline`, con las mismas
piezas de la clase de automatización:

| Función | Qué hace |
|---|---|
| `leer_configuracion` | Parámetros explícitos: `--fuente`, `--sin-descarga`, `--muestra` (widgets en Databricks) |
| `ejecutar_etapa` | Corre una etapa, mide su duración y registra `OK`, `OK_CON_FALLAS` o `ERROR` en JSON |
| `ejecutar_pipeline` | Encadena 01 adquisición → 02 Silver → 03 validación → 04 Gold → 05 modelo → 06 rentabilidad (escenario); si una falla, las siguientes no corren |

Cada corrida queda en `reports/tables/registro_pipeline.json` con su `run_id`. Repetir la
corrida con los mismos insumos da el mismo resultado: las escrituras son `overwrite` y la
descarga usa la caché. En Databricks el notebook 05 se puede programar como **Job** (ver
`docs/databricks.md`).

### El modelo dentro de la arquitectura

```
denue_establecimientos ─┐
metropolis_municipios ──┼─ construir_panel (Spark) ─► panel_municipios (Gold, 421 municipios × 4 versiones)
                        │                                   │ toPandas: ya es chico
metropolis ─────────────┤                          construir_ejemplos (variables en t, crecimiento t→t+1)
                        │                                   │
                        │                    evaluar_modelo: entrena 2015→2019 y 2019→2022, prueba 2022→2026
                        │                                   │ contra persistencia y promedio
importaciones_anuales ──┴─ parametros_mercado ─► proyectar_potencial (2026→2030, US$) ─► validar_potencial
                                                            │
                                         potencial_metropolis (Gold) · mapa_potencial.html
```

Spark hace todo lo que toca los 2.77 millones de establecimientos; el modelo trabaja sobre el
panel agregado (unos 1,700 renglones), donde pandas y XGBoost en un solo nodo bastan.

### La rentabilidad dentro de la arquitectura

```
panel_municipios (4 segmentos) ─► ejemplos_segmentos (municipio × periodo × segmento)
                                        │
                     evaluar: XGBoost compartido vs uno por segmento vs persistencia y promedio (2022→2026)
                                        │ gana el de menor MAE
                     proyectar (2026→2030) ─► locales por metrópoli y segmento   ◄── DATO REAL PROYECTADO
                                        │
config: rentabilidad ─► matriz_supuestos (calibrada con los kg de Comtrade)      ◄── SUPUESTOS SIMULADOS
                                        │
                     calcular ─► validar ─► rentabilidad_* (Gold) · rentabilidad_escenario.xlsx · heatmap
```

La frontera entre lo que se predice y lo que se supone es deliberada: el modelo solo aprende de
datos reales (locales del DENUE); los números de la empresa, que no tenemos, son supuestos
editables que multiplican esa proyección. El Excel repite el cálculo con fórmulas vivas para que
cualquiera cambie un supuesto sin tocar código.

### Colaboración

```
issue (#N) ──► rama N-descripcion ──► commits (autor: Diego Zamora,
                                       Co-authored-by: Diego Pérez)
                                          │
                                          ▼
                                pull request "Closes #N" ──► revisión y aprobación de
                                                             Diego Pérez ──► merge a main
                                                                                 │
                                                       Databricks: Git folder ──► Pull ──► Run all
```

Detalle de la convención en `CONTRIBUTING.md`.

### Qué cambiaría con más volumen

| Punto | Hoy | Con más volumen |
|---|---|---|
| Modelo | XGBoost en un nodo sobre ~1,700 renglones | `xgboost.spark` o MLflow en Databricks si el panel fuera por colonia o AGEB |
| Panel | Se recalcula completo en cada corrida | `MERGE` incremental por versión del DENUE (clase de cargas incrementales) |
| Heatmap | Un HTML con los datos adentro (~50 KB) | Un dashboard de Databricks SQL sobre `potencial_metropolis` |
