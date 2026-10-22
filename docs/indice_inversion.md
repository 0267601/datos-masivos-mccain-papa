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

   Las tres primeras son los **restaurantes objetivo**: donde se sirven papas a la francesa.
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

*Se completan con la corrida de la descarga real.*

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
