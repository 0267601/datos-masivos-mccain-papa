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

## Resultados con la descarga real

Corrida del 21 de octubre de 2026 con la descarga completa de las fuentes
(`python -m src.ingestion.download_data`): SIAP 2013–2025 (13 CSV, 72.3 MB) y SNIIM
enero–diciembre 2025 (72 páginas, 0 truncadas, 0 errores). Marciana no tiene
cotizaciones en 2025, así que el SNIIM aporta 5 variedades.

**36 reglas: 34 OK, 2 AVISO, 0 FALLA.**

| tabla | regla | estado | detalle |
|---|---|---|---|
| siap / sniim / canalizacion | columnas requeridas | OK | 13, 12 y 6 columnas presentes |
| siap / sniim / canalizacion | tipos de datos | OK | todas con el tipo esperado |
| siap | nulos (estado, produccion_ton, sup_sembrada_ha, rendimiento_ton_ha, precio_medio_rural) | OK | 0 de 3,171 |
| sniim | nulos (fecha, precio_frec, precio_min, precio_max, origen) | OK | 0 de 11,010 |
| siap / sniim / canalizacion | duplicados en la llave | OK | 0 |
| siap | rangos de producción, superficie y rendimiento | OK | 0 fuera de rango |
| siap | sup_cosechada_ha ≤ sup_sembrada_ha | OK | 0 filas |
| siap | producción = cosechada × rendimiento | OK | 0 de 3,166 filas fuera de ±5% |
| sniim | rango de precio_frec | **AVISO** | 1 de 11,010 fuera de [1, 100] $/kg |
| sniim | precio_min ≤ precio_frec ≤ precio_max | OK | 0 filas |
| siap | años esperados | OK | 2013–2025 sin huecos |
| sniim | años esperados | OK | 2025 |
| sniim | fechas consistentes | OK | 0 en el futuro, 0 en la partición equivocada |
| sniim | cobertura mensual por variedad | OK | las 5 variedades cotizan los 12 meses |
| siap × sniim | orígenes SNIIM con producción en SIAP | **AVISO** | Nayarit y Yucatán aparecen como origen y no producen papa en SIAP |
| canalizacion | participaciones suman 100% | OK | — |
| siap | volumen por año | OK | todos los años en [150, 400] filas |
| sniim | volumen mínimo | OK | 11,010 ≥ 1,000 |
| sniim raw | particiones truncadas o con error | OK | 0 truncadas, 0 con error de descarga |

La tabla completa, regla por regla, está en `reports/tables/validacion.md`.

### Cómo interpretamos los avisos

1. **Un precio de $0.43/kg.** Es una sola cotización: papa Alpha en caja de 40 kg, de
   Puebla a la Central de Abasto de Mérida, el 11 de septiembre de 2025, con mínimo,
   frecuente y máximo iguales a 0.43. El resto de la Alpha se cotiza entre 6 y
   35 $/kg (mediana 18) y las otras 55 cotizaciones en caja de 40 kg, entre 16 y
   28.5 $/kg, así que lo tratamos como error de captura del SNIIM (probablemente $4.30 o el precio
   de otra unidad). No la borramos: una fila entre 11,010 no mueve ningún resultado,
   y preferimos que el dataset refleje la fuente y que el aviso quede documentado.
2. **Nayarit y Yucatán como origen.** Es el mismo fenómeno que Coahuila en el E1: el
   SNIIM registra el origen que declara el comerciante, que puede ser el estado desde
   donde se redistribuye. No los quitamos: en el índice de canalización quedan con
   producción 0 y canalización negativa (−1.9 y −2.4), que es justo la señal de
   redistribución.

### Con la muestra

Con `--muestra` (SIAP completo, SNIIM enero–marzo, 2,683 filas) el resultado también es
34 OK, 2 AVISO y 0 FALLA, pero los avisos cambian: el de precios no aparece (la
cotización de $0.43 es de septiembre) y aparece uno de truncamiento, porque la muestra
no se descarga y no deja manifiesto que revisar.

`--muestra` se niega a sobrescribir una descarga real: si `data/raw/` ya tiene archivos
descargados, el comando termina con código 1 sin tocar nada. Lo agregamos después de
que una corrida del notebook en modo muestra reemplazó nuestra copia cruda del SIAP.

## Problemas que encontramos al validar y que ya corrige el procesamiento

| Problema | Dónde | Corrección |
|---|---|---|
| La capital aparece con cuatro nombres: "Ciudad de México", "Ciudad de México / DF" (SIAP, algunos años), "Distrito Federal" (origen SNIIM) y "DF" (destino SNIIM) | `src/processing/siap.py` | `ESTADOS_EQUIVALENTES` los unifica en "Ciudad de México" |
| "Baja California " con espacio antes de los dos puntos en el destino del SNIIM | `src/processing/sniim.py` | `trim` al separar estado y mercado |
| Acentos inconsistentes entre fuentes | `src/processing/siap.py` | Llave `estado_llave` sin acentos ni mayúsculas para cruzar |
| Encabezados que cambian entre años | `src/processing/siap.py` | Lectura año por año y `unionByName` |

## Reglas del índice de inversión

`src/validation/reglas_indice.py` aplica las mismas categorías a las tablas nuevas. Cada grupo
se valida solo si sus tablas existen, así el E1 y el índice se pueden correr por separado.

| Tabla | Regla | Nivel si no se cumple |
|---|---|---|
| denue | columnas y tipos | FALLA |
| denue | nulos en nombre de actividad (≤1%), latitud y personal (≤5%) | FALLA |
| denue | sin duplicados en (`version`, `id`) | FALLA |
| denue | `cve_mun5` de 5 dígitos | FALLA |
| denue | coordenadas dentro de México | AVISO |
| denue | están las 4 versiones configuradas, una fecha de corte por versión | FALLA |
| denue | al menos 300,000 establecimientos por versión | FALLA |
| denue | cada categoría objetivo tiene negocios en cada versión | FALLA |
| denue | al menos 100 sucursales de cadenas por versión | AVISO |
| metropolis | 92 metrópolis; municipios sin duplicados; población > 0; todo municipio con su metrópoli | AVISO / FALLA |
| importaciones | tipos, nulos, duplicados en (`periodo`, `pais_codigo`) | FALLA |
| importaciones | precio entre 0.3 y 5 US$/kg | AVISO |
| importaciones | están los 132 meses; el total "Mundo" coincide con la suma de países (±5%) | AVISO |
| indice | entre 0 y 100; una fila por metrópoli; pesos suman 1 | FALLA / AVISO |

Resultado con la descarga real en Databricks: **69 reglas en total (E1 + índice): 66 OK,
3 AVISO, 0 FALLA.** Todas las reglas del DENUE, CONAPO y el índice dan OK: las 4 versiones
presentes, más de 300 mil establecimientos por versión, 92 metrópolis, 132 de 132 meses de
Comtrade y el total "Mundo" igual a la suma de países. El aviso nuevo es de Comtrade: algún
registro mes-país con precio por kilo fuera de [0.3, 5] US$/kg. El resumen anual de los tres
orígenes principales queda entre 0.69 y 1.84 US$/kg.

En la primera corrida salió una FALLA en "particiones truncadas o con error" del SNIIM que en
realidad eran los dos CSV de CONAPO (datos.gob.mx respondió 403 a Databricks). Corregimos la
regla para que solo cuente errores del SNIIM y subimos los CSV de CONAPO a mano al Volume.
