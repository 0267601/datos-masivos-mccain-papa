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
