# Decisiones técnicas

Cada decisión importante con su razón, las alternativas que consideramos y lo que nos
cuesta.

---

## Decisión
Usar Parquet como formato de `interim` y `processed`.

## Razón
Reduce el tamaño y permite lectura por columnas. En la comparación de desempeño, con un
millón de filas el Parquet pesó 11 veces menos que el CSV y pandas lo leyó 3.7 veces
más rápido. Además conserva los tipos: no hay que volver a convertir fechas y precios.

## Alternativas consideradas
CSV y JSON.

## Consecuencias
Se requiere `pyarrow`. Los archivos no se pueden abrir en Excel directamente.

---

## Decisión
Procesar con PySpark en modo local, aunque nuestro volumen cabe en pandas.

## Razón
Es la herramienta del curso y nos deja crecer (más años, más variedades, todos los
cultivos del SIAP) sin reescribir el pipeline. Lo usamos para lo que aporta:
particionamiento, partition pruning, broadcast y window functions.

## Alternativas consideradas
pandas (más rápido a nuestro volumen) y DuckDB.

## Consecuencias
Requiere Java. Spark tarda ~0.25 s de costo fijo por consulta; a nuestro volumen pandas
es más rápido y lo documentamos en `docs/desempeno.md`.

---

## Decisión
Guardar los datos crudos sin modificar: el CSV completo del SIAP (todos los cultivos) y
el HTML del SNIIM, no los registros ya filtrados.

## Razón
Si una transformación sale mal, la rehacemos sin volver a pedirle nada a la fuente. Los
portales se caen seguido y el SNIIM responde 503 cuando lo consultamos rápido.

## Alternativas consideradas
Guardar solo papa (como en el E1) o solo los registros parseados del SNIIM.

## Consecuencias
`data/raw/` pesa ~110 MB (72 MB del SIAP y el resto en HTML del SNIIM) en lugar de 2 MB. Por eso no se sube a GitHub.

---

## Decisión
Leer el SIAP año por año y unir con `unionByName`, en lugar de
`spark.read.csv("data/raw/siap/")`.

## Razón
Spark toma el encabezado del primer archivo para todos. Como los encabezados cambian
entre años (`Precio` → `Preciomediorural`, `Nomcultivo Sin Um` → `Nomcultivo`), las
columnas quedarían desalineadas **sin ningún error visible**.

## Alternativas consideradas
Leer todo junto y corregir después; definir un esquema fijo.

## Consecuencias
Un `read` por año. Con 13 años no se nota.

---

## Decisión
Parsear el HTML del SNIIM dentro de Spark con `read.text(wholetext=True)` y una UDF de
BeautifulSoup.

## Razón
El parseo se reparte entre las particiones. Con 72 archivos da igual; con varios años deja
de ser un cuello de botella. Al principio usamos `wholeTextFiles` + `flatMap`, pero esa es la
API de RDD y Databricks serverless no la expone; con la API de DataFrame el mismo código corre
local y en Databricks.

## Alternativas consideradas
`wholeTextFiles` + `flatMap` (RDD); un ciclo de Python y luego `createDataFrame` (lo que
hacíamos en el E1).

## Consecuencias
BeautifulSoup tiene que estar instalado donde corran los workers. En serverless los workers
no ven la carpeta `src/`, así que le pedimos a cloudpickle que mande el código del parser
completo junto con la UDF (`register_pickle_by_value`).

---

## Decisión
Particionar `sniim_precios` por `anio` y `mes`, y `siap_produccion` por `anio`.

## Razón
Casi todas nuestras consultas filtran por periodo. Con particiones, Spark ignora las
carpetas que no corresponden (`PartitionFilters` en el plan).

## Alternativas consideradas
No particionar; particionar por variedad o por estado.

## Consecuencias
Más archivos pequeños. Por variedad o estado quedarían particiones muy desbalanceadas
(Alpha concentra ~90% de las cotizaciones).

---

## Decisión
Escribir cada capa en disco y que cada etapa lea la anterior desde disco.

## Razón
Podemos rehacer solo `processed` si cambia la limpieza, y cualquiera puede inspeccionar
`interim` para depurar.

## Alternativas consideradas
Encadenar todo en memoria en un solo plan de Spark.

## Consecuencias
Un poco más de escritura; a nuestro volumen son segundos.

---

## Decisión
Unificar los cuatro nombres de la capital y cruzar estados con una llave sin acentos.

## Razón
La capital aparece como "Ciudad de México", "Ciudad de México / DF" (SIAP, algunos
años), "Distrito Federal" (origen SNIIM) y "DF" (destino SNIIM). Sin unificar, el cruce
la parte en varios estados.

## Alternativas consideradas
Catálogo de claves INEGI.

## Consecuencias
La tabla de equivalencias se mantiene a mano en `src/processing/siap.py`. Si una fuente
inventa otro nombre, la validación lo marca como AVISO.

---

## Decisión
Validación con tres niveles: OK, AVISO y FALLA.

## Razón
Algunas anomalías son propias de la fuente (un estado que cotiza sin producir) y no
deben detener el pipeline, pero sí quedar documentadas. Las fallas reales (duplicados,
tipos, años faltantes) sí lo detienen con código de salida 1.

## Alternativas consideradas
Todo o nada; usar Great Expectations.

## Consecuencias
Hay que leer los avisos en `docs/validation.md`. Great Expectations sería otra
dependencia grande para 36 reglas.

---

## Decisión
Desactivar el modo ANSI de Spark (`spark.sql.ansi.enabled = false`).

## Razón
Spark 4 lo activa por defecto: un texto no numérico en un `cast` truena en lugar de dar
nulo. Lo apagamos para que el pipeline se comporte igual en Spark 3.5 (Colab) y 4.x, y
contamos los nulos en la validación.

## Alternativas consideradas
`try_cast` columna por columna.

## Consecuencias
Un valor raro se vuelve nulo en silencio; por eso las reglas de nulos son estrictas en
las columnas llave.

---

## Decisión
Incluir la opción `--muestra`, que reconstruye `data/raw/` en el formato crudo de cada
fuente a partir de las copias del E1 que ya están en `data/`.

## Razón
El pipeline completo se puede evaluar sin internet y sin depender de que el SIAP o el
SNIIM estén arriba.

## Alternativas consideradas
Subir los crudos completos (~110 MB); subir una copia cruda reducida; depender siempre
de la descarga.

## Consecuencias
La muestra es reducida (SNIIM solo enero–marzo) y está documentada como tal en
`data/sample/README.md`. El SIAP reconstruido solo trae papa, no todos los cultivos.

---

## Decisión
Configuración en `config/config.example.yaml`, sin credenciales.

## Razón
Años, variedades, pausas y umbrales cambian sin tocar código. Ninguna fuente pide
credenciales; si alguna llega a pedirlas, irán por variable de entorno.

## Alternativas consideradas
Constantes en el código (como en el E1).

## Consecuencias
Dependencia de PyYAML. `config/config.yaml` local está en `.gitignore`.

---

## Decisión
Usar un Volume de Unity Catalog como data lake en Databricks y registrar la capa
`processed` como tablas Delta.

## Razón
El Volume guarda las capas como archivos (CSV, HTML y Parquet), igual que `data/` en local,
así que el pipeline no cambia: solo le pasamos otra raíz con la variable de entorno
`DATOS_MASIVOS_DATA`. Las tablas Delta permiten consultar el resultado con SQL desde el
workspace sin saber dónde están los archivos.

## Alternativas consideradas
DBFS (`dbfs:/FileStore`), que Databricks ya no recomienda; guardar todo solo como tablas
Delta, que no sirve para la copia cruda en HTML.

## Consecuencias
El notebook `03_databricks.ipynb` crea el esquema `workspace.mccain_papa` y el Volume
`datalake` la primera vez. En serverless no hay `cache()` ni `spark.stop()`; el código los
omite cuando detecta Databricks (`src/utils/spark.py`).

---

## Decisión
Que `--muestra` se niegue a sobrescribir una descarga real.

## Razón
Una corrida del notebook en modo muestra reemplazó nuestra copia cruda del SIAP (72 MB) por
la muestra (0.4 MB). La copia cruda es justo lo que debemos conservar sin modificar.

## Alternativas consideradas
Escribir la muestra en otra carpeta; solo advertir en el log.

## Consecuencias
Si `data/raw/` ya tiene una descarga real, `--muestra` termina con código 1 sin tocar nada.
Para reemplazarla hay que pedirlo explícitamente con `--muestra --forzar`.
