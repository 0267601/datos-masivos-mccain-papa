# Datos Masivos 2026 — McCain: ¿dónde se vende la papa industrial en México?

Entregable 1 — Definición del problema, fuentes de datos y repositorio reproducible.

## Integrantes

Proyecto en parejas.

| Nombre | Matrícula |
|---|---|
| Diego Zamora González | 0267601 |
| Diego Pérez Palafox | 0269250 |

## 1. Problema

McCain opera en México desde 1995 y su producto central es la papa procesada
(papa a la francesa congelada). Esa papa requiere variedad industrial —tipo
Russet o Alpha— con alta materia seca y bajos azúcares reductores, no papa de
mesa genérica.

El problema que atacamos es que **no existe una visión pública de dónde se vende
papa industrial en México**. Las estadísticas agrícolas reportan cuánto se
produce, pero no por qué canal se comercializa. Un estado puede producir mucha papa y
aun así no ser un proveedor disponible, porque su volumen ya está
comprometido por contrato, o porque su producción se está extinguiendo.

Nos parece relevante porque la industria de papa procesada en México depende de
importación. Cualquier estrategia de abasto local necesita primero identificar
qué estados tienen producción **no comprometida con el mercado spot**.

### Pregunta concreta

> ¿Qué estados mexicanos venden su papa fuera del mercado mayorista público,
> y cuáles de ellos están creciendo lo suficiente para sustituir importación
> de papa industrial?

## 2. Usuario e interesado

**Usuario:** equipo de abasto agrícola y desarrollo de proveedores de una
procesadora de papa (caso McCain México).

**Decisión que habilita:** a qué estados dirigir contratos agrícolas y
programas de desarrollo de proveedores, y en cuáles no vale la pena invertir
porque su producción está en contracción.

## 3. Fuentes de datos

### Fuente 1 — SIAP, Cierre de la Producción Agrícola

| | |
|---|---|
| **Origen** | Servicio de Información Agroalimentaria y Pesquera (SADER) |
| **URL** | https://nube.agricultura.gob.mx/datosAbiertos/Agricola.php |
| **Variables** | Estado, municipio, ciclo, modalidad, superficie sembrada/cosechada/siniestrada, volumen de producción, rendimiento, precio medio rural, valor de producción |
| **Periodo** | 2003–2025 (anual, cifras definitivas) |
| **Formato** | CSV, codificación latin-1 |
| **Obtención** | **Descarga automatizada por código** (`src/siap_extractor.py`) |

### Fuente 2 — SNIIM, Precios de Mercados Nacionales

| | |
|---|---|
| **Origen** | Sistema Nacional de Información e Integración de Mercados (Secretaría de Economía) |
| **URL** | http://www.economia-sniim.gob.mx |
| **Variables** | Fecha, variedad de papa, presentación, estado de origen, mercado destino, precio mínimo, máximo y frecuente |
| **Periodo** | Al menos 2010–2025 (diario, días hábiles) |
| **Formato** | HTML (tablas) |
| **Obtención** | **Scraping por código** con requests + BeautifulSoup (`src/sniim_extractor.py`) |

Obtenemos las dos fuentes mediante código. Ninguna nos pide credenciales ni token.

## 4. Cómo ejecutar

### Opción A — Local (VS Code o Jupyter)

```bash
git clone https://github.com/0267601/datos-masivos-mccain-papa.git
cd datos-masivos-mccain-papa
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
jupyter notebook notebooks/entregable_1.ipynb
```

Ejecutamos el notebook de arriba abajo.

### Opción B — Google Colab

Abrimos `notebooks/entregable_1.ipynb` en Colab. La primera celda detecta Colab,
clona el repositorio e instala las dependencias automáticamente.

### Notas de reproducibilidad

- El notebook **no depende de rutas personales**: resolvemos las rutas a partir
  de la ubicación del repositorio, así que corre igual en la computadora de
  cualquiera de nosotros y en la del profesor.
- Descargamos los datos de la fuente al ejecutar. Dejamos los CSV de `data/`
  como copias de respaldo pequeñas, para que el entregable se pueda revisar
  aunque el portal esté caído.
- No usamos contraseñas, tokens ni llaves de API. Si más adelante agregamos una
  fuente con credenciales, la configuraremos por variable de entorno y lo
  documentaremos aquí.

## 5. Estructura

```
datos-masivos-mccain-papa/
├── README.md
├── requirements.txt
├── .gitignore
├── notebooks/
│   └── entregable_1.ipynb      Análisis completo del entregable
├── src/
│   ├── siap_extractor.py       Descarga automatizada de SIAP
│   └── sniim_extractor.py      Scraping de SNIIM con BeautifulSoup
├── data/
│   ├── siap_papa_2013_2025.csv     Muestra de respaldo
│   ├── sniim_papa_2025.csv         Muestra de respaldo
│   └── procedencia.json            Filtros, fecha de extracción, conteos
└── docs/
    └── hallazgos.md            Resumen de resultados
```

## 6. Limitaciones conocidas

- **SNIIM reporta el origen declarado en el mercado, no el estado productor.**
  Coahuila aparece con presencia mayorista muy superior a su producción porque
  la Central de Abasto de La Laguna redistribuye papa de otros estados.
- **SIAP no distingue variedad.** El cruce con SNIIM lo compensa parcialmente,
  porque SNIIM sí separa Alpha, Rouset, Galeana, Gema, Marciana y San José.
- **SIAP cambia encabezados entre años** (schema drift). Los normalizamos en
  `src/siap_extractor.py`.
- **SNIIM trunca a 1000 registros sin avisar.** Particionamos por mes para
  evitarlo y marcamos cada partición con una bandera `truncado`. Sin esto
  perdíamos el 89.8% de los datos sin ningún error visible.
