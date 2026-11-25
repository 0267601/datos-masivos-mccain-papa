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

---

## Decisión
Cambiar la pregunta de "dónde se vende la papa mexicana" a "en qué zonas metropolitanas
invertir para vender más papas a la francesa".

## Razón
McCain no compra papa en México: importa papa congelada de variedades industriales. El E1 lo
confirmó (la Russet es 99.6% importada). Saber qué estados venden por contrato no lleva a una
decisión de McCain; saber dónde crece la demanda sí.

## Alternativas consideradas
Seguir con la canalización y agregar más años; buscar productores para sustituir importación,
que McCain no tiene en planes.

## Consecuencias
Sumamos DENUE, CONAPO y Comtrade. El pipeline del E1 se queda como está y corre por separado
(`--fuente e1`); el índice corre con `--fuente indice`.

---

## Decisión
Clasificar los restaurantes por el nombre de la actividad y no por el código SCIAN.

## Razón
El DENUE 2015 usa SCIAN 2013 y desde 2019 usa SCIAN 2018. El nombre de las clases que nos
importan ("pizzas, hamburguesas, hot dogs…", "autoservicio", "a la carta") se conserva entre
versiones; los códigos no necesariamente.

## Alternativas consideradas
Una tabla de equivalencias SCIAN 2013 → 2018.

## Consecuencias
Las palabras clave están en `config/config.example.yaml` (`denue.categorias`). Si INEGI
cambia el nombre de una clase, se ajusta ahí sin tocar código.

---

## Decisión
Calcular el crecimiento entre noviembre 2019 y mayo 2026, aunque bajamos también 2015 y 2022.

## Razón
2019 y 2026 tienen la misma clasificación (SCIAN 2018) y el mismo formato, así que la
diferencia es crecimiento y no cambio de método. 2015 y 2022 quedan en `serie_metropolis`
para ver la tendencia y el efecto de la pandemia.

## Alternativas consideradas
2015 contra 2026 (más años, pero mezcla dos clasificaciones).

## Consecuencias
`indice.version_inicial` y `indice.version_final` en la configuración; se pueden cambiar.

---

## Decisión
Convertir cada componente en percentil y promediar con el mismo peso.

## Razón
Los componentes tienen escalas muy distintas (miles de restaurantes contra tasas de 3%). Con
percentiles, cada uno va de 0 a 100 y un valor extremo (la Ciudad de México en tamaño) no
aplasta a los demás. Pesos iguales porque no tenemos datos de ventas para estimarlos.

## Alternativas consideradas
Estandarizar con z-score (sensible a extremos); pesos elegidos a mano.

## Consecuencias
El índice ordena, no mide cuánto más conviene una ciudad que otra. Los pesos están en la
configuración (`indice.pesos`) para que el usuario los ajuste.

---

## Decisión
Usar UN Comtrade para las importaciones, en lugar de Data México.

## Razón
La API de datos de Data México respondió 502 en todas nuestras pruebas. Comtrade tiene una
API pública sin llave, trae valor y kilos (con eso sacamos precio por kilo) y separa por país
de origen.

## Alternativas consideradas
Data México; el SIAVI de la Secretaría de Economía (sin API).

## Consecuencias
La API pública acepta un periodo por consulta: pedimos mes por mes (132 consultas) con pausa y
reintentos, igual que el particionado del SNIIM. Comtrade no separa por ciudad, así que la
competencia queda a nivel nacional.

---

## Decisión
Dejar *restaurantes de autoservicio* fuera de los restaurantes objetivo.

## Razón
Con la descarga real, su conteo nacional salta sin lógica entre versiones del DENUE: 4,502
(2015), 20,670 (2019), 10,296 (2022) y 16,605 (2026). Las otras dos categorías crecen de forma
estable (comida rápida 61 mil → 80 mil, a la carta 60 mil → 73 mil entre 2019 y 2026). El salto
parece un cambio de clasificación de INEGI; como 2019 es el año base del crecimiento, castigaba a
las metrópolis donde hubo más reclasificación.

## Alternativas consideradas
Mantenerla y advertirlo; usar 2022 como año base.

## Consecuencias
`denue.categorias_objetivo` queda en comida rápida y a la carta. La categoría se sigue
calculando y queda en `serie_metropolis` para quien la quiera revisar.

---

## Decisión
Agregar un modelo XGBoost que proyecte el crecimiento de restaurantes objetivo por municipio
de 2026 a 2030.

## Razón
El índice dice dónde está la oportunidad hoy; para decidir una inversión de varios años hace
falta saber dónde va a crecer. El DENUE trae cuatro versiones (2015, 2019, 2022, 2026), así
que podemos aprender de tres periodos de crecimiento reales. XGBoost maneja relaciones no
lineales (un municipio turístico chico no crece como uno industrial grande), tolera valores
faltantes (el periodo 2015→2019 no tiene crecimiento previo) y da la contribución de cada
variable a cada predicción.

## Alternativas consideradas
Regresión lineal (no captura interacciones), repetir el crecimiento anterior (persistencia) o
el promedio. Las dos últimas las conservamos como referencias: el modelo solo vale si les gana
en un periodo que no vio.

## Consecuencias
Validación temporal: entrenamos con 2015→2019 y 2019→2022 y probamos con 2022→2026. El
periodo de prueba incluye la recuperación pospandemia, distinta a los anteriores; por eso
reportamos el error contra las referencias y no lo escondemos. Parámetros en
`config/config.example.yaml` (`modelo`).

---

## Decisión
Medir la "rentabilidad" como potencial de mercado en dólares, no como utilidad.

## Razón
No tenemos ventas, precios de venta, márgenes ni costos de McCain, y no usamos datos internos
en un repositorio público. Lo que sí podemos medir con datos abiertos es el tamaño del
mercado: restaurantes objetivo × kg de papa congelada importada por restaurante × US$/kg
(UN Comtrade, último año completo).

## Alternativas consideradas
Un índice de rentabilidad con márgenes supuestos (inventaría números); quedarnos solo con el
crecimiento predicho (no dice cuánto vale).

## Consecuencias
Supone que el consumo por restaurante y el precio por kilo se mantienen, y que todo el
mercado es papa importada. El potencial ordena metrópolis por tamaño de oportunidad; para
hablar de rentabilidad hace falta cruzarlo con datos internos fuera del repositorio.

---

## Decisión
Entrenar el modelo por municipio, no por metrópoli, y usar pandas para esa parte.

## Razón
Con 92 metrópolis y tres periodos habría 276 ejemplos; por municipio son unos 1,200, y luego
sumamos los restaurantes proyectados a su metrópoli. El panel ya agregado cabe de sobra en
memoria: Spark lo construye a partir de 2.77 millones de establecimientos y de ahí en
adelante un solo nodo basta.

## Alternativas consideradas
`xgboost.spark` (distribuido): innecesario para 1,700 renglones y más lento por el costo de
arranque.

## Consecuencias
Municipios con menos de 10 restaurantes objetivo no se usan como etiqueta (su tasa es ruido)
pero sí se proyectan. Pesamos cada ejemplo por la raíz del número de restaurantes.

---

## Decisión
Separar rechazos y duplicados del DENUE en lugar de solo descartarlos.

## Razón
Lo vimos en la clase de capas Bronze/Silver/Gold: limpiar no siempre significa eliminar, y
una tabla de rechazos permite auditar qué quedó fuera. Con la huella sha256 la elección entre
duplicados es reproducible.

## Consecuencias
Nueva tabla `denue_rechazos` con `motivos_rechazo`, regla de conciliación
(interim = silver + rechazos + duplicados) y regla de validación sobre la proporción de
rechazos.

---

## Decisión
Agregar un escenario de rentabilidad por segmento con supuestos simulados, separado del modelo.

## Razón
El potencial dice cuánto vale el mercado, no cuánto le deja a McCain. A cada tipo de cliente se le
vende distinto: a las cadenas (cuentas nacionales) con descuento por volumen y margen bajo, a los
independientes a precio de lista con margen alto, a los hoteles por contrato. No tenemos esos
números de la empresa, y un modelo no puede aprender algo que nosotros inventamos. Por eso el
XGBoost solo predice lo que sí está en los datos (locales por segmento) y los precios, descuentos,
costos y participación van como supuestos editables que multiplican esa proyección.

## Alternativas consideradas
Entrenar el XGBoost con márgenes simulados (aprendería nuestros supuestos, no la realidad); usar
datos internos de McCain (no van en un repositorio público).

## Consecuencias
La tabla y el Excel se rotulan siempre como **escenario simulado**. Lo que calculamos es **margen de
contribución** (ventas menos costos variables por kilo), no utilidad neta: no incluye costos fijos,
la inversión para entrar a cada ciudad ni impuestos. El flete lo paga el cliente. El consumo por
local se calibra para que el país sume los kilos reales que importa México; solo la proporción entre
segmentos es supuesto. Con los números reales se cambia la hoja Supuestos y todo se recalcula.

---

## Decisión
Proyectar los locales por segmento con el XGBoost que gane entre uno compartido y uno por segmento.

## Razón
Un modelo por segmento es más fácil de explicar, pero las cadenas tienen pocas sucursales por
municipio (unas 2,400 en todo el país) y un modelo solo con ellas aprendería ruido. Uno compartido,
con el segmento como variable, aprende lo común (saturación, población) con todos los datos y se
divide por segmento donde los datos lo sostienen. En vez de elegir por intuición, los ponemos a
competir en el mismo periodo de prueba (2022→2026), por segmento, contra persistencia y promedio.

## Alternativas consideradas
Repartir el crecimiento total según la mezcla actual de cada ciudad (no distingue que las cadenas o
los hoteles crecen a otro ritmo).

## Consecuencias
`reports/tables/modelo_segmentos_metricas.csv` guarda la comparación y el ganador. Un segmento con
menos de 30 ejemplos de entrenamiento usa el modelo compartido aunque gane el separado. La etiqueta
de cadenas y hoteles exige al menos 5 locales al inicio del periodo; la de independientes, 10.

---

## Decisión
Responder "si McCain tuviera X millones" con una curva de respuesta por ciudad y un reparto óptimo.

## Razón
El XGBoost proyecta cuánto crece cada mercado sin importar lo que haga McCain; no sabe que invertir
el doble en Querétaro no da el doble. Hace falta una curva con rendimientos decrecientes: con
inversión se gana participación hasta un tope (más alto en los locales nuevos, que aún no tienen
proveedor), y cada monto adicional gana menos que el anterior (con la inversión *mitad* se gana 50%
del tope, con el doble 75%). Con curvas así, el mejor reparto es mandar cada dólar a la ciudad donde
rinde más, lo que equivale a invertir en cada ciudad hasta que el último dólar rinda lo mismo en
todas. El modelo alimenta la curva: el tamaño del mercado y cuántos locales nuevos habrá en 2030.

## Alternativas consideradas
Repartir proporcional al potencial (ignora que el dinero rinde menos donde ya se invirtió mucho);
un optimizador con solver (no se puede usar desde el Excel sin complementos).

## Consecuencias
Techos, costo de la mitad y costo de entrada son supuestos (`inversion` en la configuración) y se
editan en la vista Inversión del heatmap, que recalcula el reparto en vivo con el mismo algoritmo
que Python. La recuperación supone que la contribución extra llega completa desde el primer año y
no descuenta el dinero en el tiempo. Las cadenas quedan fuera porque se negocian como cuenta
nacional.

---

## Decisión
La participación de McCain cambia por segmento y por ciudad, y entrar a una ciudad tiene un costo
proporcional a su mercado: US$50 mil fijos + 5% del mercado anual.

## Razón
Con una sola participación para todo el país, todas las ciudades tenían el mismo hueco por ganar y
el reparto solo dependía del tamaño. Con participación por segmento (cadenas 50%, independientes
25%, hoteles 30%), cada ciudad tiene su propia penetración según su mezcla, y si se conoce la real
se fija a mano: donde McCain ya vende mucho, queda poco por ganar. El costo de entrada evita
repartir migajas en 92 ciudades; un monto fijo igual para todas favorecía siempre a las grandes
(pesaba menos en ellas), así que crece con el mercado: una ciudad grande pide más vendedores,
distribución y promoción para empezar.

## Alternativas consideradas
Entrada fija de US$150 mil (las grandes salían siempre primero); una participación única para el
país (todas las ciudades iguales); elegir ciudades quitando una por una la que menos deja (daba
resultados distintos según el orden y no coincidía entre Python y el HTML).

## Consecuencias
Con el costo de entrada el problema deja de ser suave: se ordenan las ciudades por el rendimiento al
que empiezan a valer la pena (umbral) y se prueba entrar a las primeras 1, 2, 3, ...; gana el grupo
que más contribución extra deja. Python y el HTML usan el mismo algoritmo y dan el mismo resultado.
Sin la penetración real de McCain, las ciudades difieren poco (25% a 29%); el resultado cambia en
cuanto se escriba la real en la vista Inversión o en `rentabilidad.participacion_por_ciudad`.

---

## Decisión
El heatmap HTML es el entregable principal; el Excel queda solo para la rentabilidad.

## Razón
Tener mercado, rentabilidad e inversión en tres archivos obligaba a saltar entre ellos. El HTML
junta las tres vistas con un selector, deja editar los supuestos y recalcula todo en la página,
sin servidor. El Excel conserva la rentabilidad con fórmulas para quien prefiera revisarla celda por
celda; el reparto con costo de entrada no cabe en fórmulas sin Solver.

## Alternativas consideradas
Mantener la hoja de inversión en el Excel con una rejilla de valores (no admite el costo de entrada
por ciudad sin macros); un tablero en Databricks (no se puede abrir sin cuenta).

## Consecuencias
El HTML lleva los datos crudos (locales por segmento y supuestos) y repite el cálculo de Python en
JavaScript; una prueba revisa que lleve esos datos y el resultado se verificó contra Python.
