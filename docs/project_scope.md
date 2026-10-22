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

> ¿Qué estados mexicanos venden su papa fuera del mercado mayorista público,
> y cuáles de ellos están creciendo lo suficiente para sustituir importación
> de papa industrial?

## Preguntas secundarias

1. ¿Qué participación tiene cada estado en la producción nacional de papa y en las
   cotizaciones mayoristas? (índice de canalización)
2. ¿Qué estados producen mucho y aparecen poco en mayoreo? Esos venden por contrato.
3. ¿Cómo evolucionó la producción de esos estados de 2013 a 2025? ¿Crecen o se contraen?
4. ¿Qué variedades se importan y a qué precio frente a la papa nacional?
5. ¿Cuál es el valor por tonelada de la papa de cada estado frente al precio de la
   papa importada?

## Unidad de análisis

El **estado-año**: la combinación de un estado y un año, donde comparamos su producción
(SIAP) contra su presencia en el mercado mayorista (SNIIM).

## ¿Qué representa una fila?

| Tabla | Una fila es | Llave |
|---|---|---|
| `siap_produccion` | La producción de papa de un municipio, en un año, un ciclo (primavera-verano u otoño-invierno) y una modalidad (riego o temporal), reportada por un CADER dentro de un DDR | `anio, estado, ddr, cader, municipio, ciclo, modalidad` |
| `sniim_precios` | Una cotización diaria: el precio de una variedad de papa, en una presentación, con un origen declarado, en un mercado destino, un día hábil | `fecha, variedad, origen, destino, presentacion` |
| `canalizacion` | Un estado en un año, con su participación en la producción nacional, su participación en las cotizaciones mayoristas y la diferencia entre ambas | `anio, estado_llave` |

Una advertencia que aprendimos en el E1: en SIAP el municipio **no** basta como llave,
porque un mismo municipio puede reportarse desde distintos CADER. Sin `ddr` y `cader`
aparecen 186 falsos duplicados.

## Alcance

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

## Limitaciones

- SNIIM reporta el **origen declarado** en el mercado, no el estado productor (Coahuila
  aparece por la redistribución de La Laguna).
- SIAP no distingue variedad: no sabemos qué parte de la producción de un estado es
  industrial.
- SNIIM trunca cada consulta a 1000 registros sin avisar; lo resolvemos partiendo por mes.
- Los portales cambian formatos entre años (encabezados, nombres de estado) y se caen con
  frecuencia.
- El índice de canalización compara **conteos de cotizaciones**, no toneladas vendidas.

## Posibles usuarios del resultado

- Equipo de **abasto agrícola y desarrollo de proveedores** de una procesadora de papa.
- Equipo de **planeación de demanda y S&OP**, para evaluar riesgo de abasto importado.
- Productores y asociaciones agrícolas que buscan compradores industriales.

## Valor esperado del proyecto

Una lista priorizada de estados donde vale la pena buscar contratos agrícolas (producen,
crecen y su papa no está comprometida en el mercado spot) y una lista de estados donde no
conviene invertir (producción en contracción). Además, un pipeline que se puede volver a
correr cada año cuando el SIAP publica el cierre agrícola.
