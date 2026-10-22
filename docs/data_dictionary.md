# Diccionario de datos

Describimos las tres tablas de `data/processed/` (Parquet). Los faltantes son los de la
corrida con la descarga real (SIAP 2013–2025, SNIIM enero–diciembre 2025).

## `siap_produccion` — producción de papa por municipio

Origen: SIAP, Cierre de la Producción Agrícola. Particionada por `anio`. 3,171 filas.

| Variable | Tipo | Descripción | Unidad | Faltantes | Observaciones |
|---|---|---|---|---|---|
| anio | int | Año agrícola del cierre | año | 0% | Partición. 2013–2025 |
| estado | string | Entidad federativa | — | 0% | Estandarizado: "Ciudad de México / DF" → "Ciudad de México" |
| estado_llave | string | Estado en minúsculas, sin acentos | — | 0% | Solo para cruzar con SNIIM |
| ddr | string | Distrito de Desarrollo Rural | — | 0% | Parte de la llave |
| cader | string | Centro de Apoyo al Desarrollo Rural | — | 0% | Parte de la llave |
| municipio | string | Municipio | — | 0% | 220 municipios distintos |
| ciclo | string | Ciclo productivo | — | 0% | Primavera-Verano, Otoño-Invierno |
| modalidad | string | Modalidad hídrica | — | 0% | Riego, Temporal |
| cultivo | string | Cultivo | — | 0% | Siempre "Papa" |
| sup_sembrada_ha | double | Superficie sembrada | ha | 0% | |
| sup_cosechada_ha | double | Superficie cosechada | ha | 0% | ≤ sembrada (validado) |
| sup_siniestrada_ha | double | Superficie perdida por siniestro | ha | 0% | |
| produccion_ton | double | Volumen de producción | toneladas | 0% | ≈ cosechada × rendimiento (validado, ±5%) |
| rendimiento_ton_ha | double | Rendimiento | t/ha | 0% | ≤ 120 (validado) |
| precio_medio_rural | double | Precio pagado al productor | $/tonelada | 0% | En 2013–2020 la fuente lo llama "Precio" |
| valor_produccion | double | Valor de la producción | $ MXN | 0% | |
| pct_siniestrada | double | Superficie siniestrada / sembrada | % | 0% | Derivada |
| valor_por_ton | double | Valor de producción / producción | $/tonelada | 0.2% | Derivada; nula cuando la producción es 0 |

## `sniim_precios` — cotizaciones diarias en mercados mayoristas

Origen: SNIIM, Precios de Mercados Nacionales. Particionada por `anio` y `mes`. 11,010 filas.

| Variable | Tipo | Descripción | Unidad | Faltantes | Observaciones |
|---|---|---|---|---|---|
| fecha | date | Día de la cotización | — | 0% | Días hábiles |
| anio, mes | int | Año y mes de la fecha | — | 0% | Particiones |
| variedad | string | Variedad de papa | — | 0% | Alpha, Galeana, Gema, Marciana, Rouset, San Jose |
| producto_id | int | ProductoId del catálogo del SNIIM | — | 0% | 740, 748, 749, 753, 766, 767 |
| presentacion | string | Empaque cotizado | — | 0% | Kilogramo, cajas, arpillas y costales de 20 a 60 kg (15 presentaciones) |
| origen | string | Origen declarado en el mercado | — | 0% | Incluye "Importación" |
| destino | string | Mercado, como "Estado: Nombre del mercado" | — | 0% | 44 mercados |
| estado_destino | string | Estado del mercado destino | — | 0% | Derivada; "DF" → "Ciudad de México" |
| mercado | string | Nombre del mercado destino | — | 0% | Derivada |
| precio_min | double | Precio mínimo del día | $/kg | 0% | |
| precio_max | double | Precio máximo del día | $/kg | 0% | |
| precio_frec | double | Precio más frecuente del día | $/kg | 0% | 6–35 $/kg; una cotización de 0.43 marcada como AVISO en la validación |
| rango_precio | double | precio_max − precio_min | $/kg | 0% | Derivada |
| obs | string | Observaciones del SNIIM | — | 97.7% | Casi siempre vacía; a veces el país de origen ("EUA") |
| es_importacion | boolean | El origen es "Importación" | — | 0% | Derivada; distinción central del análisis |
| estado_origen | string | Origen estandarizado | — | 5.5% | Nulo a propósito cuando es importación |
| estado_origen_llave | string | Origen en minúsculas, sin acentos | — | 5.5% | Para cruzar con SIAP |
| estado_destino_llave | string | Destino en minúsculas, sin acentos | — | 0% | |
| archivo | string | HTML crudo del que salió la fila | — | 0% | Trazabilidad hacia data/raw/ |

## `canalizacion` — índice por estado y año

Cruce de las dos tablas anteriores. Solo años presentes en ambas fuentes. 23 filas.

| Variable | Tipo | Descripción | Unidad | Faltantes | Observaciones |
|---|---|---|---|---|---|
| anio | int | Año | año | 0% | |
| ranking | int | Posición por canalización dentro del año | — | 0% | 1 = más canalizado fuera del spot |
| estado | string | Estado | — | 0% | |
| estado_llave | string | Llave de cruce | — | 0% | |
| produccion_ton | double | Producción del estado (SIAP) | toneladas | 0% | 0 si cotiza pero no produce |
| pct_nacional | double | Participación en la producción nacional | % | 0% | Suma 100 por año (validado) |
| cotizaciones | int | Cotizaciones nacionales con ese origen (SNIIM) | conteo | 0% | Excluye importación |
| pct_mayoreo | double | Participación en las cotizaciones | % | 0% | Suma 100 por año (validado) |
| canalizacion | double | pct_nacional − pct_mayoreo | puntos % | 0% | Positivo: vende por contrato |
| precio_rural_ton | double | Valor de producción / producción | $/tonelada | 13.0% | Nulo si no produce |
| precio_frec_promedio | double | Promedio de precio_frec de sus cotizaciones | $/kg | 26.1% | Nulo si no cotiza |

## Capas intermedias

| Capa | Contenido |
|---|---|
| `data/raw/siap/siap_cierre_<anio>.csv` | CSV completo del portal, todos los cultivos, latin-1, sin modificar |
| `data/raw/sniim/<variedad>/sniim_<id>_<anio>-<mes>.html` | Página de resultados del SNIIM, sin modificar |
| `data/raw/manifiesto.json` | url, parámetros, bytes, sha256, fecha y estado por archivo |
| `data/interim/siap_papa/` | SIAP con encabezados unificados y solo papa; todo como texto |
| `data/interim/sniim_precios/` | Registros parseados del HTML; todo como texto |

---

# Tablas del índice de inversión

## `denue_establecimientos` — negocios del sector 72

Origen: DENUE de INEGI, sector 72. Particionada por `version`. Una fila por establecimiento y
versión; llave (`version`, `id`).

| Variable | Tipo | Descripción | Observaciones |
|---|---|---|---|
| version | string | Versión del DENUE | Partición: 2015_02, 2019_11, 2022_11, 2026_05 |
| fecha_corte | date | Fecha de la versión | De la configuración |
| id | string | Identificador del establecimiento | |
| nom_estab | string | Nombre del establecimiento | |
| nombre_act | string | Clase de actividad | En 2015 se llama "Nombre de clase de la actividad" |
| categoria | string | comida_rapida, autoservicio, a_la_carta, bares, hoteles, otros | Derivada del nombre de la actividad |
| cadena | string | Cadena detectada (McDonald's, KFC…) | Nula si no es cadena |
| per_ocu / personal_min | string / int | Estrato de personal ocupado y su límite inferior | "11 a 30 personas" → 11 |
| cve_mun5 | string | Clave de municipio a 5 dígitos | Llave con CONAPO |
| latitud, longitud | double | Coordenadas | |

## `metropolis` y `metropolis_municipios` — catálogo CONAPO 2020

| Variable | Tipo | Descripción |
|---|---|---|
| clave_metropoli | string | Clave de la zona metropolitana (92) |
| tipo_metropoli | string | Zona metropolitana, metrópoli municipal o zona conurbada |
| metropoli | string | Nombre |
| cve_mun5 | string | Municipio que la forma (solo en `metropolis_municipios`) |
| poblacion_2020 | long | Población 2020 |
| pib, pib_per_capita | double | Producto interno bruto y por habitante (solo en `metropolis`) |

## `importaciones` — papa congelada importada por mes y país

Origen: UN Comtrade, HS 200410. Llave (`periodo`, `pais_codigo`).

| Variable | Tipo | Descripción | Unidad |
|---|---|---|---|
| periodo | string | Mes | YYYYMM |
| anio, mes | int | Año y mes | |
| pais_codigo, pais | int, string | País de origen | Código M49; 0 = Mundo (total) |
| valor_usd | double | Valor importado | US$ |
| kg | double | Peso neto | kg |
| usd_kg | double | Precio por kilo | US$/kg |
| es_total | boolean | Fila del total "Mundo" | Se excluye al sumar países |

`importaciones_anuales` resume por año y país, con `participacion` (% del valor del año).

## `indice_inversion` — una fila por zona metropolitana

| Variable | Tipo | Descripción |
|---|---|---|
| ranking | int | Posición (1 = mejor) |
| indice | double | Promedio ponderado de los puntajes, 0–100 |
| puntaje_crecimiento, puntaje_cadenas, puntaje_turismo, puntaje_mercado | double | Percentil de cada componente, 0–100 |
| crecimiento_anual | double | Tasa anual compuesta de restaurantes objetivo, % |
| cadenas_netas_100k | double | Aperturas netas de cadenas por 100 mil habitantes |
| hoteles_100k | double | Hoteles por 100 mil habitantes |
| objetivo_inicial, objetivo_final | long | Restaurantes objetivo en cada versión |
| restaurantes_100k | double | Restaurantes objetivo por 100 mil habitantes, versión final |

`serie_metropolis` trae los conteos por metrópoli y versión (las cuatro), y
`cadenas_metropolis` las sucursales por cadena.
