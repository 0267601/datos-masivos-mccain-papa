# Arquitectura

## Arquitectura real (entregable 2)

```
SIAP: CSV por año (latin-1)          SNIIM: HTML por variedad y mes
            │                                      │
            └────────── src/ingestion/download_data ──────────┘
                         requests · caché · reintentos · manifiesto
                                      │
                               data/raw/   ← copia cruda, sin modificar
                                      │
                        src/processing/build_dataset  (PySpark)
                                      │
               ┌──────────────────────┴───────────────────────┐
     SIAP: leer año por año,                       SNIIM: wholeTextFiles +
     unificar encabezados, unionByName,            flatMap(BeautifulSoup)
     filtrar papa                                  en paralelo
               └──────────────────────┬───────────────────────┘
                                      │
                             data/interim/   ← esquema unificado, todo texto (Parquet)
                                      │
                 tipos · nombres · estados · duplicados · nulos · variables
                                      │
                             data/processed/  ← Parquet
                     siap_produccion/  (anio=)
                     sniim_precios/    (anio=/mes=)
                     canalizacion/     (cruce SIAP × SNIIM, broadcast + window)
                                      │
                       src/validation/validate_dataset
                         36 reglas · OK / AVISO / FALLA
                                      │
                              Dataset preparado
                                      │
                     src/processing/benchmark (desempeño)
```

## Dónde se almacenan los datos

| Capa | Ruta | Formato | ¿Se sube a GitHub? |
|---|---|---|---|
| raw | `data/raw/` | CSV y HTML tal como llegan, más `manifiesto.json` | No |
| interim | `data/interim/` | Parquet, todo como texto | No |
| processed | `data/processed/` | Parquet tipado y particionado | No |
| sample | `data/sample/` y copias del E1 en `data/` | CSV pequeños; `--muestra` reconstruye raw a partir de las copias | Sí |
| resultados | `reports/tables/`, `reports/figures/` | CSV, Markdown, PNG | Sí |

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
| Parquet | Formato de interim y processed |
| PyYAML | Configuración sin tocar código |
| logging | `logs/pipeline.log` con cada descarga y cada error |
| pytest | 33 pruebas sin internet |
| pandas | Solo al final, para tablas chicas y la comparación de desempeño |

## Qué podría dejar de funcionar al crecer el volumen

| Punto | Por qué | Qué haríamos |
|---|---|---|
| Descarga del SNIIM | Es secuencial con pausa de 1 s: 6 variedades × 12 meses × N años | Paralelizar con pocos hilos sin saturar el portal |
| Tope de 1000 registros | Un mes con muchos mercados podría tocarlo | Partir por quincena; el manifiesto ya avisa cuál |
| CSV crudo del SIAP | Trae todos los cultivos (~6 MB por año) y Spark no puede empujar filtros a un CSV | Convertir raw a Parquet una vez y filtrar ahí |
| `toPandas()` | Lleva todo al driver | Solo lo usamos en tablas agregadas |
| Full outer join del cruce | Genera shuffle | Hoy es diminuto; si creciera, particionar por año antes |
