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
