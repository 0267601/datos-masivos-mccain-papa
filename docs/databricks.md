# Ejecución en Databricks

Corremos el mismo pipeline del repositorio en Databricks (Free Edition, cómputo serverless).
El código se queda en GitHub; Databricks lo abre como **Git folder** y las capas de datos
viven en un **Volume de Unity Catalog**, que es nuestro data lake.

```
GitHub ──Git folder──▶ Databricks ──notebooks/03_databricks.ipynb──▶ Unity Catalog
                                                                    workspace.mccain_papa
                                                                    ├── datalake (Volume)
                                                                    │   ├── raw/
                                                                    │   ├── interim/
                                                                    │   ├── processed/
                                                                    │   └── reports/
                                                                    ├── siap_produccion (Delta)
                                                                    ├── sniim_precios   (Delta)
                                                                    └── canalizacion    (Delta)
```

## 1. Abrir el repositorio en Databricks

1. En el menú izquierdo: **Workspace**.
2. Entramos a nuestra carpeta de usuario: **Users → nuestro correo**.
3. Botón **Create** (arriba a la derecha) → **Git folder**.
4. En *Git repository URL* pegamos:
   `https://github.com/0267601/datos-masivos-mccain-papa.git`
   *Git provider* se llena solo (GitHub). Damos **Create Git folder**.

El repositorio es público, así que no hace falta conectar credenciales para clonarlo.

## 2. Correr el pipeline

1. Dentro del Git folder: `notebooks/03_databricks.ipynb`.
2. Arriba a la derecha, **Connect → Serverless**.
3. **Run all**.

El notebook:

| Sección | Qué hace |
|---|---|
| 0 | Instala `beautifulsoup4` y `pyyaml` (no instala `pyspark`: ya lo trae Databricks) |
| 1 | Crea el esquema `workspace.mccain_papa` y el Volume `datalake` si no existen, y apunta el pipeline ahí |
| 2 | Revisa si alcanza los portales y elige el modo de adquisición |
| 3 | `build_dataset`: raw → interim → processed dentro del Volume |
| 4 | `validate_dataset`: 36 reglas; resultados en `datalake/reports/tables/` |
| 5 | Muestra la estructura del data lake |
| 6 | Registra `processed` como tablas Delta en `workspace.mccain_papa` |
| 7 | Consulta el índice de canalización con SQL |
| 8 | Plan con `PartitionFilters` sobre el Volume |

## 3. Si Databricks no alcanza los portales

Los portales del gobierno a veces bloquean conexiones desde la nube. El notebook lo detecta
y elige otro modo:

- **`zip`** (recomendado): subimos nuestra descarga real hecha en local.
  1. En la terminal, dentro del repo:
     ```bash
     cd data && zip -qr raw.zip raw && cd ..
     ```
  2. En Databricks: **Catalog → workspace → mccain_papa → datalake → Upload to this volume**
     y subimos `data/raw.zip`.
  3. Volvemos a correr el notebook: encuentra `raw.zip` y lo descomprime en `raw/`.
- **`muestra`**: si no hay `raw.zip`, reconstruye `raw/` desde las copias del E1
  (SNIIM solo enero–marzo).

## 4. Ver el data lake y las tablas

- **Archivos:** Catalog → workspace → mccain_papa → **datalake**. Ahí se navega por `raw/`,
  `interim/`, `processed/` y `reports/`.
- **Tablas:** Catalog → workspace → mccain_papa → `canalizacion`, `siap_produccion`,
  `sniim_precios`. La pestaña **Sample Data** muestra filas; **Create → Query** abre el
  editor de SQL.

Ejemplo de consulta:

```sql
SELECT estado, ROUND(canalizacion, 1) AS canalizacion
FROM workspace.mccain_papa.canalizacion
WHERE anio = 2025
ORDER BY canalizacion DESC;
```

## 5. Actualizar después de un cambio en GitHub

En el Git folder: botón con el nombre de la rama (`main`) → **Pull**. Luego **Run all** otra vez.
Cada etapa sobrescribe su capa, así que correrlo varias veces no duplica datos.

## Diferencias con la ejecución local

| | Local | Databricks serverless |
|---|---|---|
| Capas | `data/` | Volume `workspace.mccain_papa.datalake` |
| Cómo se elige | Por defecto | Variables `DATOS_MASIVOS_DATA` y `DATOS_MASIVOS_REPORTS` (las pone el notebook) |
| SparkSession | `local[*]`, la creamos nosotros | La de la plataforma; no la cerramos |
| `cache()` | Sí | No disponible; el código la omite |
| Registro | `logs/pipeline.log` | Salida del notebook y `/tmp/datos_masivos_logs/` |
| Comparación de desempeño | `python -m src.processing.benchmark` | No se corre (mide pandas contra Spark local) |

## Problemas conocidos

- **`ModuleNotFoundError: bs4` dentro de una UDF.** En el panel **Environment** (ícono a la
  derecha del notebook) agregamos `beautifulsoup4` en *Dependencies* y damos **Apply**.
- **`SCHEMA_NOT_FOUND` o permisos.** En Free Edition el catálogo es `workspace`; si el
  workspace usa otro, cambiamos `CATALOGO` en la sección 1 del notebook.
