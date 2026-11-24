# Cómo contribuimos

## Flujo de trabajo

1. **Un issue por tarea.** Antes de programar, abrimos un issue que diga qué se va a
   hacer y cómo sabremos que está terminado.
2. **Una rama por issue**, con el número y una descripción corta:

   ```bash
   git checkout main
   git pull
   git checkout -b 12-validacion-rangos
   ```

3. **Commits chicos y en español**, en presente y con el verbo al inicio, como los que
   ya tenemos:

   ```
   Agrega la validacion de rangos de precio
   Corrige la deteccion de Colab, que tronaba en un entorno limpio
   Quita el mensaje de stderr de matplotlib de las salidas del notebook
   ```

4. **Pull request hacia `main`** que cierre el issue (`Closes #12` en la descripción).
5. **Revisión del otro integrante** antes de hacer merge. Nadie hace merge de su propio
   PR sin revisión.

## Quién hace qué

| Paso | Diego Zamora (0267601) | Diego Pérez (0269250-eng) |
|---|---|---|
| Issue | Lo abre y se lo asigna | Comenta si falta algo |
| Rama y commits | Los sube desde la terminal | — |
| Coautoría | Agrega a Diego Pérez como coautor en los commits que trabajamos juntos | Aparece como coautor en esos commits |
| Pull request | Lo abre con `Closes #N` y pide revisión a `0269250-eng` | Revisa *Files changed*, comenta y **aprueba** |
| Merge | — | Hace el merge cuando aprobó |
| Databricks | **Pull** en el Git folder y **Run all** | Revisa la salida |

### Coautoría en los commits

GitHub reconoce a un coautor por la última línea del mensaje, separada por una línea en
blanco:

```
Agrega el modelo de potencial por metropoli

Co-authored-by: Diego Pérez Palafox <0269250@up.edu.mx>
```

Así el commit cuenta para los dos en el historial y en la lista de contribuidores. El correo
tiene que ser el verificado en la cuenta de GitHub de Diego Pérez.

### Revisión desde la web (sin instalar nada)

1. Abrir el PR en `github.com/0267601/datos-masivos-mccain-papa/pulls`.
2. **Files changed** → revisar → **Review changes** → **Approve** → **Submit review**.
3. **Conversation** → **Merge pull request** → **Confirm merge**.

Para subir un archivo propio desde la web (por ejemplo, su reflexión): **Add file →
Upload files**, elegir *Create a new branch for this commit and start a pull request* y
pedir revisión a `0267601`.

## Antes de abrir un pull request

```bash
pytest                                        # las pruebas deben pasar
python -m src.ingestion.download_data --muestra
python -m src.processing.build_dataset
python -m src.validation.validate_dataset     # 0 FALLA
```

## Estilo de código

- Nombres de funciones, variables y comentarios **en español**, sin acentos en el
  código (`anio`, `particion`); con acentos en Markdown.
- Comentarios en primera persona del plural y explicando **por qué**, no qué:
  `# Declaramos latin-1 explicitamente: el portal no lo anuncia en la cabecera.`
- Constantes en MAYÚSCULAS al inicio del módulo.
- Docstring de módulo con el pipeline en una línea: `fuente -> ... -> salida`.
- Rutas siempre desde `src/utils/rutas.py`; nunca rutas personales.
- Spark con cadenas de métodos entre paréntesis, un método por línea.
- `toPandas()` solo sobre tablas agregadas y chicas.

## Qué no se sube

- Datos de `data/raw/`, `data/interim/` y `data/processed/` (se regeneran).
- `config/config.yaml`, `.env` o cualquier credencial.
- Salidas de notebooks con rutas locales o mensajes de error.

## Si hay conflictos

```bash
git checkout mi-rama
git fetch origin
git merge origin/main        # resolvemos los conflictos en el editor
pytest                       # confirmamos que todo sigue pasando
git commit
git push
```
