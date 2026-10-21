# Muestra de datos

`raw/`, `interim/` y `processed/` no se suben: se regeneran con el pipeline y están en
`.gitignore`. Lo que sí está en el repositorio:

## Muestras pequeñas para revisar la estructura

| Archivo | Contenido | Filas |
|---|---|---|
| `siap_papa_2025_muestra.csv` | Producción de papa por municipio, SIAP 2025 | 100 |
| `sniim_papa_2025_muestra.csv` | Cotizaciones mayoristas, 17 por variedad, SNIIM 2025 | 85 |

Tienen las columnas de la extracción del entregable 1 (ver `docs/data_dictionary.md`).

## Muestra completa para correr el pipeline sin internet

Las copias de respaldo del entregable 1 siguen en `data/`:

| Archivo | Contenido | Filas |
|---|---|---|
| `data/siap_papa_2013_2025.csv` | SIAP, solo papa, 2013–2025 | 3,171 |
| `data/sniim_papa_2025.csv` | SNIIM, 5 variedades con cotizaciones, 2025 | 11,010 |
| `data/procedencia.json` | Filtros, fecha de extracción y conteos del E1 | — |

Con ellas, `--muestra` reconstruye `data/raw/` en el formato crudo de cada fuente y el
pipeline corre completo sin tocar los portales:

```bash
python -m src.ingestion.download_data --muestra
python -m src.processing.build_dataset
python -m src.validation.validate_dataset
```

La reconstrucción (`src/ingestion/muestra.py`) conserva los problemas reales de las
fuentes para que el pipeline los tenga que resolver:

- **SIAP**: un CSV por año en latin-1, con el *schema drift* de los encabezados (hasta
  2015 `Precio`; de 2016 a 2020 `Nomcultivo Sin Um`; desde 2021 `Preciomediorural`).
- **SNIIM**: un HTML por variedad y mes con `table#tblResultados`, dos filas de
  encabezado y celdas `td.Datos2`. Solo enero–marzo, para que sea ligera.

Diferencias con la descarga real: el SIAP real trae todos los cultivos (~6 MB por año)
y aquí solo papa; el HTML real trae además las tablas de diseño del portal.
