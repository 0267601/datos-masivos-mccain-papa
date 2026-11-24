# Alcance del proyecto

## Contexto del problema

McCain opera en México desde 1995 y su producto central es la papa a la francesa
congelada. Esa papa requiere **variedad industrial** —tipo Russet o Alpha— con alta
materia seca y bajos azúcares reductores. La industria de papa procesada en México
depende de importación: en el entregable 1 encontramos que la Russet que cotiza el
SNIIM es 99.6% importada y solo se vende en la Central de Abasto de Tijuana.

Cualquier estrategia de abasto local necesita saber **dónde está la papa que no pasa por
el mercado mayorista**. Las estadísticas agrícolas (SIAP) dicen cuánto produce cada
estado, pero no por qué canal se vende; los precios de mercado (SNIIM) dicen qué se vende
en las centrales de abasto, pero no cuánto se produce. Ninguna fuente sola responde la
pregunta; cruzadas, sí.

## Pregunta principal

> Si McCain tuviera X millones para crecer en México, ¿en qué zonas metropolitanas
> los pondría, y cuánto crecería ahí el mercado de papas a la francesa?

**Usuario:** equipo comercial, de expansión y de planeación de demanda de McCain México.
**Decisión que habilita:** en qué ciudades abrir distribución, fuerza de ventas o
promociones primero.

### Cómo llegamos a esta pregunta

Empezamos con una pregunta de abasto (entregables 1 y 2): *¿qué estados venden su papa fuera
del mercado mayorista y podrían sustituir importación?* El cruce SIAP × SNIIM la respondió
(Sonora, +28.1 de canalización) y mostró algo más importante: la Russet, la variedad
industrial, es 99.6% importada. McCain no compra papa mexicana, así que movimos la pregunta
del lado de la oferta al de la demanda. El pipeline del E1 se conserva y corre igual
(`--fuente e1`).

## Preguntas secundarias

1. ¿Qué metrópolis tienen más restaurantes donde se venden papas a la francesa y cuáles
   crecieron más entre 2019 y 2026? (índice, componente A)
2. ¿Dónde están abriendo las cadenas que venden papas (McDonald's, KFC, Carl's Jr.…)?
   (componente B)
3. ¿Qué metrópolis tienen un perfil turístico que sume consumo de visitantes?
4. ¿De qué país viene la papa congelada que importa México, a qué precio y cómo cambió?
   (pieza C, competencia)
5. ¿Dónde crecerá más el mercado de 2026 a 2030 y cuánto vale ese crecimiento en dólares?
   (modelo XGBoost)
6. ¿El modelo predice mejor que suponer que todo sigue igual?

## Unidad de análisis

La **zona metropolitana** (92, según CONAPO 2020). El modelo trabaja un nivel más abajo, el
**municipio metropolitano en una versión del DENUE**, y suma sus resultados a la metrópoli.

## ¿Qué representa una fila?

| Tabla | Una fila es | Llave |
|---|---|---|
| `siap_produccion` | La producción de papa de un municipio, en un año, un ciclo (primavera-verano u otoño-invierno) y una modalidad (riego o temporal), reportada por un CADER dentro de un DDR | `anio, estado, ddr, cader, municipio, ciclo, modalidad` |
| `sniim_precios` | Una cotización diaria: el precio de una variedad de papa, en una presentación, con un origen declarado, en un mercado destino, un día hábil | `fecha, variedad, origen, destino, presentacion` |
| `canalizacion` | Un estado en un año, con su participación en la producción nacional, su participación en las cotizaciones mayoristas y la diferencia entre ambas | `anio, estado_llave` |
| `denue_establecimientos` | Un restaurante, hotel o bar del sector 72 en una versión del DENUE | `version, id` |
| `metropolis_municipios` | Un municipio que forma parte de una zona metropolitana | `cve_mun5` |
| `importaciones` | Lo que México importó de papa congelada en un mes desde un país | `periodo, pais_codigo` |
| `indice_inversion` | Una zona metropolitana con sus cuatro componentes y su índice | `clave_metropoli` |
| `panel_municipios` | Un municipio metropolitano en una versión del DENUE, con sus conteos | `cve_mun5, version` |
| `potencial_metropolis` | Una zona metropolitana con su crecimiento esperado y su potencial en US$ | `clave_metropoli` |

Una advertencia que aprendimos en el E1: en SIAP el municipio **no** basta como llave,
porque un mismo municipio puede reportarse desde distintos CADER. Sin `ddr` y `cader`
aparecen 186 falsos duplicados.

## Alcance del pipeline de abasto (entregables 1 y 2)

**Incluye**

- Papa (todas las variedades) en SIAP, 2013–2025, a nivel municipal.
- Seis variedades de papa en SNIIM (Alpha, Galeana, Gema, Marciana, Rouset, San José),
  todos los mercados nacionales, 2025.
- Un pipeline reproducible que descarga, procesa, valida y deja el dataset listo.

**No incluye**

- Precios de papa procesada (papa a la francesa) ni datos internos de McCain.
- Contratos agrícolas reales: los inferimos por ausencia en el mercado spot.
- Comercio exterior (importaciones en volumen): solo vemos la papa importada que llega a
  mercados mayoristas.

## Limitaciones de SIAP y SNIIM

- SNIIM reporta el **origen declarado** en el mercado, no el estado productor (Coahuila
  aparece por la redistribución de La Laguna).
- SIAP no distingue variedad: no sabemos qué parte de la producción de un estado es
  industrial.
- SNIIM trunca cada consulta a 1000 registros sin avisar; lo resolvemos partiendo por mes.
- Los portales cambian formatos entre años (encabezados, nombres de estado) y se caen con
  frecuencia.
- El índice de canalización compara **conteos de cotizaciones**, no toneladas vendidas.

## Alcance del índice y del modelo

**Incluye:** DENUE sector 72 en cuatro versiones (2015–2026), las 92 metrópolis de CONAPO,
importaciones mensuales de papa congelada 2015–2025 (UN Comtrade) y un modelo que proyecta a
2030. **No incluye:** ventas, márgenes ni datos internos de McCain; consumo en hogares;
municipios fuera de una zona metropolitana.

**Limitaciones:** el DENUE cuenta locales, no ventas; el potencial supone que el consumo por
restaurante y el precio por kilo se mantienen; la competencia es nacional (Comtrade no separa
por ciudad).

## Posibles usuarios del resultado

- Equipo **comercial y de expansión**: en qué ciudades crecer primero.
- Equipo de **abasto agrícola y desarrollo de proveedores** de una procesadora de papa.
- Equipo de **planeación de demanda y S&OP**, para evaluar riesgo de abasto importado.
- Productores y asociaciones agrícolas que buscan compradores industriales.

## Valor esperado del proyecto

Una lista priorizada de las 92 zonas metropolitanas con dos lecturas: dónde está la
oportunidad hoy (índice de 0 a 100) y dónde crecerá el mercado de aquí a 2030 (potencial en
US$ proyectado con XGBoost), con un heatmap interactivo que explica cada metrópoli. Además,
un pipeline que se vuelve a correr con un comando cuando INEGI publique la siguiente versión
del DENUE, y el análisis de abasto del E1 (estados que venden por contrato) como antecedente.
