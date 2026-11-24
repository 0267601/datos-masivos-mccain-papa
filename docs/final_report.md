# Reporte final

**¿Dónde invertir para vender más papas a la francesa en México?**
Diego Zamora González (0267601) · Diego Pérez Palafox (0269250) · Datos Masivos 2026

## Resumen ejecutivo

La oportunidad de McCain en México está en **ciudades medianas turísticas o industriales**, no
en las tres grandes metrópolis. Con 2.77 millones de establecimientos del DENUE (cuatro
versiones, 2015–2026), el catálogo de 92 zonas metropolitanas de CONAPO y diez años de
importaciones de UN Comtrade, construimos:

1. Un **índice de inversión** de 0 a 100 por metrópoli. Lo encabezan Los Cabos (82.7),
   Acapulco (73.4) y Guanajuato (73.1). El Valle de México, Monterrey y Guadalajara no entran
   al top 15.
2. Un **modelo XGBoost** que proyecta el crecimiento de restaurantes de 2026 a 2030 y lo
   traduce a **potencial de mercado en US$**, con un heatmap interactivo que explica cada
   metrópoli.
3. Una lectura de la **competencia**: la papa congelada importada se triplicó desde 2019
   (US$148 M → US$455 M en 2025) y Canadá pasó de 10% a 39% del valor importado.

Todo corre con un comando (`python main.py`) y en Databricks, con el data lake en Unity
Catalog, 69 reglas de validación (66 OK, 3 AVISO, 0 FALLA) y 68 pruebas automáticas.

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

## Procesamiento

| Etapa | Resultado |
|---|---|
| raw → interim (DENUE) | 7 zip → 2,774,445 filas; cada versión leída con su codificación y unida con `unionByName` |
| interim → Silver | 2,774,429 establecimientos; 0 sin llave, 16 duplicados (huella sha256); rechazos en `denue_rechazos` |
| Gold | `indice_inversion` (92), `serie_metropolis` (368), `panel_municipios`, `potencial_metropolis` |
| Tiempo | Procesar el DENUE completo: ~50 s en Databricks serverless; pipeline completo con caché: ~5 min |

Técnicas: Parquet particionado por versión (partition pruning comprobado en el plan),
broadcast join del catálogo de 421 municipios, `percent_rank` con ventana, UDF para el HTML
del SNIIM, conciliación interim = silver + rechazos + duplicados.

## Validación

69 reglas en ocho categorías (columnas, tipos, faltantes, duplicados, rangos, temporal,
entre tablas, volumen): **66 OK, 3 AVISO, 0 FALLA** con los datos reales. Los avisos son una
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

*Pendiente de la corrida real del notebook 05 en Databricks: métricas del modelo contra las
referencias, top 10 por potencial incremental a 2030, importancia de variables y el heatmap
`reports/figures/mapa_potencial.html`.*

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
- El modelo aprende de tres periodos, uno con la pandemia; la población es del censo 2020.
- La competencia es nacional: Comtrade no separa por ciudad ni por empresa.
- Pesos iguales en el índice es una decisión, no un hallazgo.

## Conclusiones

1. **Ciudades medianas primero.** Turísticas (Los Cabos, Acapulco, Guanajuato, Oaxaca) e
   industriales en expansión (La Laguna, Querétaro, Saltillo, León).
2. **Para una primera inversión, tamaño y crecimiento juntos:** La Laguna, Querétaro y
   Acapulco tienen más de 1,400 restaurantes objetivo y crecen.
3. **El mercado importado crece más rápido que los restaurantes** (se triplicó desde 2019
   contra +25% de restaurantes objetivo): cada restaurante usa más papa congelada.
4. **La competencia cambió:** Canadá ganó participación y es el origen más barato en 2025;
   Bélgica ya tiene 12%.

## Trabajo futuro

- Calibrar los pesos del índice y el potencial con ventas internas por ciudad, fuera del
  repositorio público.
- Seguir cada establecimiento por `id` entre versiones para separar aperturas y cierres.
- Carga incremental (`MERGE`) cuando INEGI publique la siguiente versión del DENUE.
- Agregar distancia logística (aduanas, centros de distribución) como costo de llegar a
  cada ciudad.
