# Índice de inversión por zona metropolitana

## Por qué cambiamos la pregunta

En el entregable 1 preguntamos qué estados venden su papa fuera del mercado mayorista. El
resultado fue consistente, pero no lleva a una decisión: **McCain no compra papa mexicana**.
Importa papa congelada hecha con variedades industriales (Russet) y la vende a restaurantes.
Lo que McCain sí decide es **dónde crecer su venta**.

> Si McCain tuviera X millones para crecer en México, ¿en qué zonas metropolitanas
> los pondría?

**Usuario:** equipo comercial y de expansión de McCain México. **Decisión que habilita:** en
qué ciudades abrir distribución, fuerza de ventas o promociones primero.

## Fuentes

Todas son abiertas y no piden llave. No usamos datos internos de McCain.

| Fuente | Qué tomamos | Cómo la obtenemos | Volumen |
|---|---|---|---|
| DENUE (INEGI) | Todos los negocios del sector 72 (restaurantes y hoteles), 4 versiones: feb 2015, nov 2019, nov 2022, may 2026 | Descarga masiva de 7 zip | ~300 MB en zip, ~1.5 GB en CSV, ~2.8 millones de filas |
| Las metrópolis de México 2020 (CONAPO) | 92 zonas metropolitanas, sus municipios, población y PIB | 2 CSV de datos.gob.mx | < 1 MB |
| UN Comtrade | Importaciones mensuales de México de papa congelada (HS 200410) por país de origen: dólares y kilos | API pública, un mes por consulta (132 consultas) | 2015–2025 |

## Cómo lo calculamos

1. **Clasificamos cada negocio del DENUE** por el nombre de su actividad (no por el código
   SCIAN, que cambió entre 2015 y 2019):
   - *comida rápida*: "pizzas, hamburguesas, hot dogs y pollos rostizados para llevar"
   - *autoservicio*: "restaurantes de autoservicio"
   - *a la carta*: "a la carta o de comida corrida"
   - *hoteles*, *bares* y *otros*

   *Comida rápida* y *a la carta* son los **restaurantes objetivo**: donde se sirven papas a
   la francesa. Dejamos fuera *autoservicio* porque su conteo salta sin explicación entre
   versiones (4,502 en 2015, 20,670 en 2019, 10,296 en 2022, 16,605 en 2026): parece un cambio
   de clasificación de INEGI, no crecimiento real, y 2019 es nuestro año base.
2. **Detectamos cadenas** que venden papas (McDonald's, Burger King, KFC, Carl's Jr., Wendy's,
   Vips, Toks…) con expresiones regulares sobre el nombre del establecimiento.
3. **Asignamos cada negocio a su metrópoli** con la clave de municipio a 5 dígitos
   (`cve_mun5`). Los negocios fuera de una metrópoli no entran al índice.
4. **Calculamos cuatro componentes** por metrópoli, comparando nov 2019 contra may 2026 (las
   dos con SCIAN 2018 y el mismo formato):

| Componente | Fórmula | Qué premia |
|---|---|---|
| A. Crecimiento | tasa anual compuesta de restaurantes objetivo | demanda que se está formando |
| B. Cadenas | (sucursales de cadenas 2026 − 2019) por 100 mil habitantes | que las cadenas ya están apostando ahí |
| Turismo | hoteles por 100 mil habitantes | consumo de visitantes |
| Mercado | restaurantes objetivo en 2026 | tamaño del mercado hoy |

5. **Convertimos cada componente en percentil** (0 a 100) entre las 92 metrópolis, para que
   ninguno domine por tener números más grandes, y **promediamos con el mismo peso** (25%
   cada uno). Los pesos están en `config/config.example.yaml` (`indice.pesos`).

La **pieza C, competencia**, es nacional porque Comtrade no separa por ciudad: muestra de qué
país vienen las papas importadas y a qué precio por kilo.

## Cómo correrlo

```bash
python -m src.ingestion.download_data --fuente indice     # ~15 min la primera vez
python -m src.processing.build_dataset --fuente indice
python -m src.analysis.indice_inversion
python -m src.validation.validate_dataset
```

En Databricks: `notebooks/04_indice_databricks.ipynb`, **Run all**.

## Salidas

| Archivo | Contenido |
|---|---|
| `reports/tables/indice_inversion.csv` | Las 92 metrópolis con ranking, índice, componentes y sus dos fortalezas principales |
| `reports/figures/ranking_metropolis.png` | Top 15, con lo que aporta cada componente |
| `reports/figures/mapa_oportunidad.png` | Crecimiento contra densidad de restaurantes; tamaño = población |
| `reports/figures/competencia_importacion.png` | Importaciones por país de origen y precio por kilo |
| `workspace.mccain_papa.indice_inversion` | La misma tabla, como tabla Delta en Databricks |

## Resultados

Corrida en Databricks serverless con la descarga real (22 de octubre de 2026).

### El pipeline

| Etapa | Resultado |
|---|---|
| DENUE | 7 zip (225 MB) → 2,774,445 filas en interim → 2,774,429 en processed (16 duplicados, 0 sin llave) |
| CONAPO | 92 metrópolis, 421 municipios |
| Comtrade | 132 de 132 meses, 527 filas mes-país |
| Procesamiento | ~1 minuto (DENUE completo en ~40 s) |
| Validación | 69 reglas: 66 OK, 3 AVISO, 0 FALLA |

Restaurantes en todo el país por versión del DENUE:

| Versión | Comida rápida | A la carta | Autoservicio | Hoteles |
|---|---|---|---|---|
| feb 2015 | 55,029 | 58,195 | 4,502 | 16,589 |
| nov 2019 | 61,176 | 60,267 | 20,670 | 17,295 |
| nov 2022 | 67,955 | 64,985 | 10,296 | 17,908 |
| may 2026 | 79,622 | 72,520 | 16,605 | 19,340 |

Comida rápida creció 30% entre 2019 y 2026 (unos 4% al año) y a la carta 20%. Autoservicio
salta sin lógica, por eso lo dejamos fuera (ver `docs/decisions.md`).

### ¿Dónde invertir? Top 10

| # | Metrópoli | Índice | Crecimiento anual | Cadenas netas / 100 mil hab. | Hoteles / 100 mil hab. | Restaurantes objetivo 2026 | Fortalezas |
|---|---|---|---|---|---|---|---|
| 1 | Los Cabos | 82.7 | 5.4% | 0.9 | 46.4 | 844 | turismo y cadenas |
| 2 | Acapulco | 73.4 | 2.8% | 0.8 | 37.1 | 1,463 | turismo y cadenas |
| 3 | Guanajuato | 73.1 | 4.3% | 3.6 | 64.8 | 347 | cadenas y turismo |
| 4 | Tecomán | 72.0 | 6.9% | 0.7 | 33.3 | 252 | crecimiento y turismo |
| 5 | Tapachula | 71.1 | 3.7% | 0.7 | 33.2 | 687 | turismo y cadenas |
| 6 | La Laguna | 69.2 | 6.4% | 0.9 | 5.7 | 1,667 | crecimiento y cadenas |
| 7 | Tlaxcala-Apizaco | 68.7 | 4.9% | 0.1 | 17.9 | 1,010 | crecimiento y turismo |
| 8 | Querétaro | 68.1 | 3.9% | 1.5 | 9.7 | 2,212 | cadenas y mercado |
| 9 | Córdoba | 67.8 | 5.5% | 0.3 | 17.3 | 633 | crecimiento y cadenas |
| 10 | Oaxaca | 66.5 | 1.7% | 0.3 | 48.9 | 1,800 | turismo y mercado |

Del 11 al 15: Mérida, Tuxtla Gutiérrez, Saltillo, León y Cuernavaca (65–66 puntos).

### Qué concluimos

1. **La oportunidad está en ciudades medianas, no en las tres grandes.** Ni el Valle de México,
   ni Monterrey, ni Guadalajara entran al top 15: puntúan alto en tamaño de mercado, pero en
   crecimiento, cadenas por habitante y turismo quedan por debajo.
2. **Hay dos perfiles de ciudad.** Turísticas (Los Cabos, Acapulco, Guanajuato, Oaxaca): muchos
   hoteles por habitante, consumo de visitantes. Industriales en expansión (La Laguna, Querétaro,
   Saltillo, León): puntajes altos en crecimiento de restaurantes y aperturas de cadenas, con
   pocos hoteles.
3. **Para una primera inversión** conviene empezar por las que combinan tamaño y crecimiento —
   La Laguna, Querétaro y Acapulco tienen más de 1,400 restaurantes objetivo cada una— y dejar las
   chicas de crecimiento alto (Tecomán, Guanajuato) para una segunda etapa.

### ¿Contra quién competimos?

| Año | Total importado | Estados Unidos | Canadá | Bélgica |
|---|---|---|---|---|
| 2015 | US$134 M | 89% · US$1.10/kg | 10% · US$1.04/kg | — |
| 2019 | US$149 M | 72% · US$1.21/kg | 23% · US$1.15/kg | 5% · US$0.94/kg |
| 2024 | US$499 M | 57% · US$1.63/kg | 31% · US$1.69/kg | 11% · US$1.35/kg |
| 2025 | US$455 M | 47% · US$1.49/kg | 39% · US$1.15/kg | 12% · US$1.22/kg |

- **El mercado de papa congelada importada se triplicó** desde 2019.
- **Estados Unidos pierde participación** (89% → 47%) y **Canadá la gana** (10% → 39%); en 2025
  Canadá es el origen más barato por kilo. Comtrade no separa por empresa, pero McCain produce en
  Canadá, así que parte de ese crecimiento es probablemente suyo.
- **Bélgica ya tiene 12%** con precios por debajo de Estados Unidos: es la competencia europea a
  vigilar.

## Limitaciones

- **El DENUE cuenta establecimientos, no ventas.** Un McDonald's y una fonda pesan igual en
  el componente de crecimiento; las cadenas tienen su propio componente por eso.
- **Clasificamos por actividad declarada.** Una taquería que vende papas no entra como
  restaurante objetivo.
- **Las cadenas se detectan por nombre.** Sucursales registradas con otra razón social o con
  el nombre mal escrito se pierden; los conteos de cadenas son un mínimo.
- **La población es del censo 2020.** Para 2026 las tasas por 100 mil habitantes están
  ligeramente infladas en las metrópolis que más crecen.
- **Pesos iguales es una decisión, no un hallazgo.** Cambiar los pesos en la configuración
  reordena el ranking; lo reportamos para que el usuario los ajuste.
- **La competencia es nacional.** No sabemos a qué ciudad llega cada importación.
