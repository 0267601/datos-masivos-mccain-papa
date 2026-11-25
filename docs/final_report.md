# Reporte final

**¿Dónde invertir para vender más papas a la francesa en México?**
Diego Zamora González (0267601) · Diego Pérez Palafox (0269250) · Datos Masivos 2026

## Resumen ejecutivo

Con 2.77 millones de establecimientos del DENUE (cuatro
versiones, 2015–2026), el catálogo de 92 zonas metropolitanas de CONAPO y diez años de
importaciones de UN Comtrade, construimos:

1. Un **índice de inversión** de 0 a 100 por metrópoli, que mide la oportunidad de hoy. Lo
   encabezan Los Cabos (82.7), Acapulco (73.4) y Guanajuato (73.1). El Valle de México,
   Monterrey y Guadalajara no entran al top 15.
2. Un **modelo XGBoost** que proyecta el crecimiento de restaurantes de 2026 a 2030 y lo
   traduce a **potencial de mercado en US$**. Le gana a las dos referencias simples (error
   11% menor que repetir el crecimiento anterior) y estima **US$32 M de mercado nuevo** en
   las 92 metrópolis al 2030 (de US$313 M a US$345 M al año).
3. Una lectura de la **competencia**: la papa congelada importada se triplicó desde 2019
   (US$148 M → US$455 M en 2025) y Canadá pasó de 10% a 39% del valor importado.

**Respuesta corta.** En dólares, el mercado nuevo está donde ya hay más restaurantes: Ciudad
de México, Guadalajara y Monterrey juntan 29% del crecimiento. Para crecer donde la
oportunidad de hoy y el crecimiento esperado coinciden, las apuestas son **ciudades medianas
industriales y capitales regionales**: Querétaro, La Laguna, Mérida, Saltillo y
Puebla-Tlaxcala están en el top 20 de las dos mediciones. Las turísticas que encabezan el
índice (Los Cabos, Acapulco) ya tienen muchos restaurantes por habitante y el modelo espera
que se frenen.

Todo corre con un comando (`python main.py`, unos 2.5 minutos en Databricks con caché), con
el data lake en Unity Catalog, 70 reglas de validación (67 OK, 3 AVISO, 0 FALLA) y 68 pruebas
automáticas. El heatmap interactivo (`reports/figures/mapa_potencial.html`) es un entregable
aparte: al hacer clic en cualquiera de las 92 metrópolis muestra sus números e insights.

## Problema

McCain no compra papa mexicana: importa papa congelada de variedades industriales (Russet) y
la vende a restaurantes. Lo que sí decide es **dónde crecer su venta**, y no existe una vista
pública de dónde está creciendo la demanda de papas a la francesa en el país.

Empezamos (entregables 1 y 2) con una pregunta de abasto: qué estados venden su papa por
contrato. El cruce SIAP × SNIIM la respondió (Sonora produce 30.8% de la papa y aparece en
2.6% de las cotizaciones mayoristas) y mostró que la Russet es 99.6% importada. Ese hallazgo
movió la pregunta al lado de la demanda.

## Pregunta principal

> Si McCain tuviera X millones para crecer en México, ¿en qué zonas metropolitanas los
> pondría, y cuánto crecería ahí el mercado de papas a la francesa?

Preguntas secundarias: crecimiento de restaurantes que venden papas, aperturas de cadenas,
perfil turístico, origen y precio de la papa importada, crecimiento esperado a 2030 y si el
modelo predice mejor que suponer que todo sigue igual (ver `docs/project_scope.md`).

## Fuentes de datos

| Fuente | Qué aporta | Obtención | Volumen |
|---|---|---|---|
| DENUE (INEGI), sector 72 | Cada restaurante, hotel y bar en 2015, 2019, 2022 y 2026 | Descarga masiva de 7 zip | 225 MB en zip, 2.77 M filas |
| Las metrópolis de México 2020 (CONAPO) | Municipios de cada zona metropolitana, población, PIB | 2 CSV de datos.gob.mx | 421 municipios, 92 metrópolis |
| UN Comtrade, HS 200410 | Importaciones mensuales de papa congelada por país | API pública, un mes por consulta | 132 meses |
| SIAP y SNIIM (antecedente) | Producción y precios mayoristas de papa | Descarga y scraping | 3,171 + 11,010 filas |

Todas son abiertas y no piden credenciales.

## Arquitectura

```
Fuentes ─► download_data ─► raw/ ─► build_dataset (PySpark) ─► interim/ (Bronze) ─► processed/ (Silver)
                                                                     │
                                         validate_dataset ◄──────────┘  (FALLA ⇒ se detiene)
                                                │
                       indice_inversion (Gold) ─┴─ modelo_potencial (Gold, XGBoost)
                                                │
                     reports/ (tablas, gráficas, heatmap) · tablas Delta en Unity Catalog
```

Local: `data/`. Databricks: Volume `workspace.mccain_papa.datalake` y tablas Delta. El mismo
código corre en los dos; solo cambia la raíz con una variable de entorno. Detalle en
`docs/architecture.md`.

## Metodología

Seguimos CRISP-DM: entender el negocio (cambio de pregunta), entender los datos (cinco
fuentes con sus problemas), prepararlos (capas Bronze/Silver/Gold), validarlos, modelar,
evaluar y desplegar (Databricks).

**Índice de inversión.** Cuatro componentes por metrópoli, convertidos a percentil (0–100) y
promediados con el mismo peso: crecimiento anual de restaurantes objetivo (comida rápida y a
la carta, nov 2019 → may 2026), aperturas netas de cadenas por 100 mil habitantes, hoteles
por 100 mil habitantes y número de restaurantes objetivo.

**Modelo de potencial.**

1. Panel por municipio metropolitano y versión del DENUE (Spark, broadcast join con CONAPO).
2. Para cada par de versiones consecutivas, las variables al inicio (restaurantes, densidad,
   proporción de comida rápida, cadenas por restaurante, hoteles, bares, personal,
   crecimiento del periodo anterior, crecimiento de población, PIB per cápita) y el
   crecimiento anual que ocurrió después (la etiqueta).
3. Validación temporal: entrenamos con 2015→2019 y 2019→2022 y probamos con 2022→2026,
   contra persistencia (repetir el crecimiento anterior) y promedio.
4. Reentrenamos con los tres periodos y proyectamos 2026 → 2030.
5. Potencial = restaurantes × kg de papa congelada importada por restaurante × US$/kg
   (último año completo de Comtrade).

**Rentabilidad por segmento (escenario simulado).**

1. Separamos cuatro segmentos del DENUE: cadenas (cuentas nacionales), comida rápida
   independiente, a la carta independiente y hoteles.
2. Proyectamos los locales de cada segmento a 2030 con XGBoost. Pusimos a competir un modelo
   compartido (el segmento entra como variable) contra uno por segmento, en la misma prueba
   2022→2026 y contra persistencia y promedio; proyecta el que menos se equivoca.
3. Una matriz de supuestos (no son datos de McCain) da a cada segmento su consumo relativo por
   local, descuento sobre lista y costo de servir. El costo del producto es el precio real de
   importación de Comtrade, y el consumo por local se calibra para que el país sume los kilos
   reales importados.
4. Margen de contribución = locales × kg por local × participación × (precio neto − costo del
   producto − costo de importación − costo de servir). El flete lo paga el cliente. No incluye
   costos fijos, inversión ni impuestos.

## Procesamiento

| Etapa | Resultado |
|---|---|
| raw → interim (DENUE) | 7 zip → 2,774,445 filas; cada versión leída con su codificación y unida con `unionByName` |
| interim → Silver | 2,774,429 establecimientos; 0 sin llave, 16 duplicados (huella sha256); rechazos en `denue_rechazos` |
| Gold | `indice_inversion` (92), `serie_metropolis` (368), `panel_municipios`, `potencial_metropolis` |
| Tiempo | Pipeline completo en Databricks serverless con caché: 2 min 26 s (adquisición 10 s, Silver 72 s, validación 35 s, Gold 17 s, modelo 13 s; `reports/tables/registro_pipeline.json`) |

Técnicas: Parquet particionado por versión (partition pruning comprobado en el plan),
broadcast join del catálogo de 421 municipios, `percent_rank` con ventana, UDF para el HTML
del SNIIM, conciliación interim = silver + rechazos + duplicados.

## Validación

70 reglas en ocho categorías (columnas, tipos, faltantes, duplicados, rangos, temporal,
entre tablas, volumen): **67 OK, 3 AVISO, 0 FALLA** con los datos reales. La regla nueva
revisa los rechazos del DENUE: 0 establecimientos rechazados y 16 duplicados exactos. Los avisos son una
cotización del SNIIM de $0.43/kg, dos estados que cotizan sin producir y algún mes-país de
Comtrade con precio fuera de rango. Antes de publicar el potencial, `validar_potencial`
comprueba que los totales cuadren. Al revisar los resultados encontramos que *autoservicio*
saltaba sin lógica entre versiones (4,502 → 20,670 → 10,296 → 16,605) y lo sacamos del
cálculo: las reglas revisan que el dato esté bien formado; que tenga sentido lo revisamos
nosotros. Detalle en `docs/validation.md`.

## Resultados

### ¿Dónde invertir hoy? Índice de inversión

| # | Metrópoli | Índice | Crecimiento anual | Restaurantes objetivo 2026 | Fortalezas |
|---|---|---|---|---|---|
| 1 | Los Cabos | 82.7 | 5.4% | 844 | turismo y cadenas |
| 2 | Acapulco | 73.4 | 2.8% | 1,463 | turismo y cadenas |
| 3 | Guanajuato | 73.1 | 4.3% | 347 | cadenas y turismo |
| 4 | Tecomán | 72.0 | 6.9% | 252 | crecimiento y turismo |
| 5 | Tapachula | 71.1 | 3.7% | 687 | turismo y cadenas |
| 6 | La Laguna | 69.2 | 6.4% | 1,667 | crecimiento y cadenas |
| 7 | Tlaxcala-Apizaco | 68.7 | 4.9% | 1,010 | crecimiento y turismo |
| 8 | Querétaro | 68.1 | 3.9% | 2,212 | cadenas y mercado |
| 9 | Córdoba | 67.8 | 5.5% | 633 | crecimiento y cadenas |
| 10 | Oaxaca | 66.5 | 1.7% | 1,800 | turismo y mercado |

### ¿Dónde crecerá el mercado? Modelo y potencial en US$

**¿El modelo sirve?** Probado con 2022→2026, un periodo que no vio (359 municipios; entrenó
con 691 ejemplos de 2015→2019 y 2019→2022):

| Modelo | MAE municipal (pp) | RMSE (pp) | MAE por metrópoli (pp) | Spearman entre metrópolis |
|---|---|---|---|---|
| **XGBoost** | **3.65** | **5.15** | **2.47** | **0.19** |
| Persistencia (repetir el crecimiento anterior) | 4.10 | 6.31 | 2.63 | 0.05 |
| Promedio (todos crecen igual) | 3.96 | 5.56 | 2.58 | — |

XGBoost se equivoca 11% menos que la persistencia y 8% menos que el promedio. La mejora es
real pero modesta, y el orden de las metrópolis lo acierta poco (Spearman 0.19). Por eso
usamos la proyección como **escenario** para comparar ciudades, no como pronóstico puntual.

**¿Qué mueve el crecimiento?** La variable que más pesa es la **densidad de restaurantes por
100 mil habitantes** (15% de la ganancia del modelo), y en contra: donde ya hay muchos
restaurantes por habitante, el modelo espera que el crecimiento se frene (saturación). Le
siguen el crecimiento del periodo anterior (13%), el crecimiento de población 2010–2020 (13%)
y los bares por habitante (11%).

**Potencial en US$.** En 2025 México importó 354 millones de kg de papa congelada por
US$460 M (US$1.30/kg). Repartido entre los 152,142 restaurantes objetivo del país, cada uno
equivale a unos **2,327 kg al año (≈US$3,000)**. Con eso, el mercado de las 92 metrópolis
pasa de US$313 M hoy a **US$345 M en 2030: US$32 M de mercado nuevo**.

| # | Metrópoli | Mercado nuevo al 2030 | Mercado 2030 | Crecimiento esperado | Restaurantes hoy | Índice (#) |
|---|---|---|---|---|---|---|
| 1 | Ciudad de México | US$5.3 M | US$73.8 M | 1.9% | 22,646 | 52.5 (#36) |
| 2 | Guadalajara | US$2.3 M | US$20.6 M | 2.9% | 6,076 | 56.3 (#31) |
| 3 | Monterrey | US$1.8 M | US$20.2 M | 2.3% | 6,101 | 56.9 (#30) |
| 4 | Puebla-Tlaxcala | US$1.3 M | US$13.5 M | 2.6% | 4,025 | 62.6 (#19) |
| 5 | Tijuana | US$1.1 M | US$8.5 M | 3.6% | 2,438 | 59.9 (#23) |
| 6 | Toluca | US$0.8 M | US$8.7 M | 2.5% | 2,592 | 42.3 (#62) |
| 7 | Mérida | US$0.8 M | US$7.4 M | 3.0% | 2,174 | 66.2 (#11) |
| 8 | Juárez | US$0.6 M | US$5.5 M | 3.0% | 1,624 | 60.1 (#22) |
| 9 | Querétaro | US$0.6 M | US$7.3 M | 2.1% | 2,212 | 68.1 (#8) |
| 10 | La Laguna | US$0.6 M | US$5.6 M | 2.8% | 1,667 | 69.2 (#6) |

**¿Coinciden el índice y el modelo?** Solo en parte. Entre el índice y el crecimiento esperado
hay correlación de rangos **negativa (−0.30)**: el índice premia a las ciudades que ya
crecieron y tienen turismo; el modelo espera que justo esas se frenen. Los Cabos pasó de 5.4%
anual (2019–2026) a 2.4% esperado; Acapulco, Tecomán y Tapachula, a menos de 1%. Diez
metrópolis están en el top 20 de las dos mediciones: Puebla-Tlaxcala, Mérida, Querétaro,
La Laguna, Cuernavaca, León, Saltillo, Tuxtla Gutiérrez, Pachuca y Oaxaca.

El heatmap interactivo (`reports/figures/mapa_potencial.html`, entregable aparte) muestra las
92 metrópolis en el mapa y en la tabla; al hacer clic en una aparecen sus números, su lugar en
el índice y qué variables movieron su predicción.

### ¿Cuánto le dejaría a McCain? Escenario de rentabilidad

*Escenario simulado: los locales por segmento son datos reales proyectados; precio, descuentos,
costos y participación son supuestos editables en `reports/rentabilidad_escenario.xlsx`.*

| Segmento | Consumo relativo | Descuento | Costo de servir | Margen por kg con lista US$2.20 |
|---|---|---|---|---|
| Cadenas (cuenta nacional) | 6.0× | 25% | US$0.05 | US$0.10 (6%) |
| Comida rápida independiente | 1.0× | 0% | US$0.25 | US$0.45 (20%) |
| A la carta independiente | 0.7× | 0% | US$0.30 | US$0.40 (18%) |
| Hoteles (contrato) | 1.5× | 15% | US$0.15 | US$0.22 (12%) |

Costo del producto: US$1.30/kg (Comtrade 2025) + US$0.20 de importación. Participación de
McCain: 30%.

*Resultados por metrópoli: pendientes de la corrida en Databricks.*

### ¿Contra quién competimos?

| Año | Total importado | Estados Unidos | Canadá | Bélgica |
|---|---|---|---|---|
| 2015 | US$134 M | 89% · US$1.10/kg | 10% · US$1.04/kg | — |
| 2019 | US$148 M | 72% · US$1.21/kg | 23% · US$1.15/kg | 5% · US$0.94/kg |
| 2024 | US$499 M | 57% · US$1.63/kg | 31% · US$1.69/kg | 11% · US$1.35/kg |
| 2025 | US$455 M | 47% · US$1.49/kg | 39% · US$1.15/kg | 12% · US$1.22/kg |

## Limitaciones

- El DENUE cuenta establecimientos, no ventas; las cadenas se detectan por nombre (mínimo).
- El potencial es tamaño de mercado, no utilidad; supone consumo por restaurante y precio
  constantes y que todo el mercado es papa importada.
- El modelo aprende de tres periodos, uno con la pandemia; la población es del censo 2020. Le
  gana a las referencias, pero ordena poco a las metrópolis (Spearman 0.19).
- El consumo por restaurante viene de dividir las importaciones nacionales entre los
  restaurantes objetivo: es un promedio, no el consumo real de cada ciudad. Como las
  importaciones crecieron más rápido que los restaurantes, el potencial probablemente se
  queda corto.
- La competencia es nacional: Comtrade no separa por ciudad ni por empresa.
- Pesos iguales en el índice es una decisión, no un hallazgo.

## Conclusiones

1. **El tamaño está en las grandes; el crecimiento relativo, en las medianas.** Ciudad de
   México, Guadalajara y Monterrey aportan 29% del mercado nuevo en dólares, pero crecen
   entre 1.9% y 2.9% anual.
2. **Si McCain tuviera X millones para crecer, los pondría primero donde coinciden el índice
   y el modelo:** Querétaro, La Laguna, Mérida, Saltillo y Puebla-Tlaxcala. Tienen más de
   1,000 restaurantes objetivo, están en el top 20 del índice y del mercado nuevo, y el
   modelo espera que crezcan entre 2.1% y 3.7% anual.
3. **Las turísticas ya están muy servidas.** Los Cabos, Acapulco y Tapachula encabezan el
   índice por su crecimiento pasado y su turismo, pero tienen tantos restaurantes por
   habitante que el modelo espera que se frenen: son para atender bien, no para apostar a
   crecimiento.
4. **El mercado importado crece más rápido que los restaurantes** (se triplicó desde 2019
   contra +25% de restaurantes objetivo): cada restaurante usa más papa congelada.
5. **La competencia cambió:** Canadá ganó participación y es el origen más barato en 2025;
   Bélgica ya tiene 12%.

## Trabajo futuro

- Calibrar los pesos del índice y el potencial con ventas internas por ciudad, fuera del
  repositorio público.
- Seguir cada establecimiento por `id` entre versiones para separar aperturas y cierres.
- Carga incremental (`MERGE`) cuando INEGI publique la siguiente versión del DENUE.
- Agregar distancia logística (aduanas, centros de distribución) como costo de llegar a
  cada ciudad.
