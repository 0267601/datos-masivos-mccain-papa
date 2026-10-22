# Validación del dataset

Corremos la validación después de construir el dataset:

```bash
python -m src.validation.validate_dataset
```

El código está en `src/validation/reglas.py` (las reglas) y
`src/validation/validate_dataset.py` (las aplica a `data/processed/`). Cada corrida
escribe `reports/tables/validacion.csv` y `reports/tables/validacion.md`.

## Tres niveles de resultado

| Estado | Qué significa | Qué hace el pipeline |
|---|---|---|
| **OK** | La regla se cumple | Sigue |
| **AVISO** | Algo raro que hay que explicar, pero no invalida el dataset | Sigue y lo documentamos aquí |
| **FALLA** | El dataset no se debe usar | Termina con código de salida 1 |

Separamos AVISO de FALLA porque algunas "anomalías" son propias de la fuente y no
errores nuestros. Por ejemplo, el SNIIM reporta el **origen declarado** en el mercado, no
el estado productor (lo documentamos en el E1): que un estado aparezca como origen sin
producir papa en SIAP es raro, pero real.

Si faltan columnas, ni siquiera corremos el resto: `exigir_columnas()` lanza
`ValueError`, igual que el ejemplo de la rúbrica.

## Reglas por categoría

| # | Categoría | Reglas | Tablas |
|---|---|---|---|
| 1 | Existencia de columnas | Columnas requeridas presentes | siap, sniim, canalización |
| 2 | Tipos de datos | `int`, `double`, `date`, `boolean`, `string` según la columna | siap, sniim, canalización |
| 3 | Valores faltantes | % máximo de nulos por columna (0% en llaves, hasta 5% en precio medio rural) | siap, sniim |
| 4 | Duplicados | Cero duplicados en la llave de cada tabla | siap, sniim, canalización |
| 5 | Rangos válidos | Producción y superficie ≥ 0; rendimiento ≤ 120 t/ha; cosechada ≤ sembrada; producción ≈ cosechada × rendimiento (±5%); precio en [1, 100] $/kg; mínimo ≤ frecuente ≤ máximo | siap, sniim |
| 6 | Consistencia temporal | Años exactamente los configurados, sin huecos; ninguna fecha futura; la partición `anio/mes` coincide con la fecha; cobertura mensual por variedad | siap, sniim |
| 7 | Consistencia entre tablas | Orígenes del SNIIM con producción en SIAP; participaciones de canalización suman 100% por año | siap × sniim, canalización |
| 8 | Volumen esperado | Filas por año de SIAP en [150, 400]; SNIIM ≥ 1,000 filas; ninguna partición del SNIIM truncada ni con error de descarga (manifiesto) | siap, sniim, raw |

Los umbrales viven en `config/config.example.yaml` (sección `validacion`), no en el código.

### Llaves que usamos

| Tabla | Llave |
|---|---|
| siap_produccion | `anio, estado, ddr, cader, municipio, ciclo, modalidad` |
| sniim_precios | `fecha, variedad, origen, destino, presentacion` |
| canalizacion | `anio, estado_llave` |

En SIAP conservamos `ddr` y `cader` a propósito: sin ellas la llave no es única y
aparecen 186 falsos duplicados (lo vimos en el E1).

## Resultados de la última corrida

Corrida con la **muestra** (`python -m src.ingestion.download_data --muestra`):
SIAP 2013–2025 completo y SNIIM enero–marzo 2025 (5 variedades: Marciana no tiene
cotizaciones en 2025).

**36 reglas: 34 OK, 2 AVISO, 0 FALLA.**

| tabla | regla | estado | detalle |
|---|---|---|---|
| siap | columnas requeridas | OK | 13 columnas presentes |
| sniim | columnas requeridas | OK | 12 columnas presentes |
| canalizacion | columnas requeridas | OK | 6 columnas presentes |
| siap / sniim / canalizacion | tipos de datos | OK | todas con el tipo esperado |
| siap | nulos (estado, produccion_ton, sup_sembrada_ha, rendimiento_ton_ha, precio_medio_rural) | OK | 0 de 3,171 |
| sniim | nulos (fecha, precio_frec, precio_min, precio_max, origen) | OK | 0 de 2,683 |
| siap / sniim / canalizacion | duplicados en la llave | OK | 0 |
| siap | rangos de producción, superficie y rendimiento | OK | 0 fuera de rango |
| siap | sup_cosechada_ha ≤ sup_sembrada_ha | OK | 0 filas |
| siap | producción = cosechada × rendimiento | OK | 0 de 3,166 filas fuera de ±5% |
| sniim | rango de precio_frec | OK | 0 fuera de [1, 100] |
| sniim | precio_min ≤ precio_frec ≤ precio_max | OK | 0 filas |
| siap | años esperados | OK | 2013–2025 sin huecos |
| sniim | años esperados | OK | 2025 |
| sniim | fechas consistentes | OK | 0 en el futuro, 0 en la partición equivocada |
| sniim | cobertura mensual por variedad | OK | las 5 variedades cotizan los 3 meses |
| siap × sniim | orígenes SNIIM con producción en SIAP | **AVISO** | Nayarit y Yucatán aparecen como origen y no producen papa en SIAP |
| canalizacion | participaciones suman 100% | OK | — |
| siap | volumen por año | OK | todos los años en [150, 400] filas |
| sniim | volumen mínimo | OK | 2,683 ≥ 1,000 |
| sniim raw | particiones truncadas | **AVISO** | con la muestra no hay manifiesto de descarga; con la descarga completa esta regla sí se evalúa |

La tabla completa, regla por regla, está en `reports/tables/validacion.md`.

### Cómo interpretamos los avisos

1. **Nayarit y Yucatán como origen.** Es el mismo fenómeno que Coahuila en el E1: el
   SNIIM registra el origen que declara el comerciante, que puede ser el estado desde
   donde se redistribuye. No los quitamos: en el índice de canalización quedan con
   producción 0 y canalización negativa, que es justo la señal de redistribución.
2. **Sin manifiesto.** La muestra se copia, no se descarga, así que no hay registro de
   truncamiento. Con `python -m src.ingestion.download_data` el manifiesto existe y la
   regla pasa a OK o FALLA.

## Problemas que encontramos al validar y que ya corrige el procesamiento

| Problema | Dónde | Corrección |
|---|---|---|
| La capital aparece con cuatro nombres: "Ciudad de México", "Ciudad de México / DF" (SIAP, algunos años), "Distrito Federal" (origen SNIIM) y "DF" (destino SNIIM) | `src/processing/siap.py` | `ESTADOS_EQUIVALENTES` los unifica en "Ciudad de México" |
| "Baja California " con espacio antes de los dos puntos en el destino del SNIIM | `src/processing/sniim.py` | `trim` al separar estado y mercado |
| Acentos inconsistentes entre fuentes | `src/processing/siap.py` | Llave `estado_llave` sin acentos ni mayúsculas para cruzar |
| Encabezados que cambian entre años | `src/processing/siap.py` | Lectura año por año y `unionByName` |
