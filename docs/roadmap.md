# Roadmap y checkpoints

Cada entrega mejora el mismo sistema: entendimos el problema, construimos el proceso e
integramos, analizamos y comunicamos.

## Línea de tiempo

| Checkpoint | Qué quedó | Evidencia | Estado |
|---|---|---|---|
| **E1 — Definición y adquisición** | Pregunta de abasto, extractores SIAP y SNIIM, exploración, diccionario, primera arquitectura | `notebooks/entregable_1.ipynb`, `docs/project_scope.md`, `docs/data_dictionary.md`, PR #8 | Hecho |
| **E2 — Pipeline y escalabilidad** | `download_data`, capas raw/interim/processed en Parquet particionado, 36 reglas, comparación de desempeño, pruebas | PR #9–#14, `docs/validation.md`, `docs/desempeno.md` | Hecho |
| **E2 — Databricks** | El mismo pipeline en serverless, data lake en Unity Catalog, tablas Delta | PR #16, #18, `notebooks/03_databricks.ipynb` | Hecho |
| **Cambio de pregunta** | De abasto a demanda: índice de inversión por metrópoli con DENUE, CONAPO y Comtrade | PR #20, `docs/indice_inversion.md`, `docs/decisions.md` | Hecho |
| **Correcciones con datos reales** | 403 de CONAPO, autoservicio fuera del cálculo, resultados reales, evidencia de Databricks | PR #22, #24, #26, #28 | Hecho |
| **Final — Integración** | `main.py` con `ejecutar_pipeline`, rechazos y conciliación, muestra real del índice | `src/pipeline.py`, `data/sample/indice/`, `reports/tables/registro_pipeline.json` | Hecho (PR #30) |
| **Final — Análisis** | Modelo XGBoost con validación temporal, potencial en US$; heatmap interactivo como entregable aparte | `src/analysis/modelo_potencial.py`, notebook 05, `reports/figures/mapa_potencial.html` | Hecho (PR #30 y resultados reales) |
| **Final — Comunicación** | Reporte final, presentación, reflexiones | `docs/final_report.md`, `docs/reflections/` | Pendiente: presentación y reflexiones |

## Pasos para cerrar

| # | Paso | Quién | Cómo | Estado |
|---|---|---|---|---|
| 1 | Subir el PR del examen final | Diego Zamora | Issue, rama `14-examen-final`, PR #30 | Hecho |
| 2 | Revisar, aprobar y hacer merge | Diego Pérez | Web: *Files changed* → *Approve* → *Merge* | Hecho |
| 3 | Correr el notebook 05 en Databricks | Diego Zamora | Git folder → **Pull** → **Run all** | Hecho (23 nov, 2 min 26 s) |
| 4 | Descargar `entrega_final.zip` | Diego Zamora | Catalog → datalake → reports → **Download** | Hecho |
| 5 | Subir resultados reales, muestra y arreglo del zip | Diego Zamora | PR con `final_report.md`, `reports/`, `data/sample/indice/` | En PR |
| 6 | Repetir **Pull** y **Run all** para que el notebook quede limpio | Diego Zamora | Todas las celdas en verde | Pendiente |
| 7 | Escribir las reflexiones | Cada uno | `docs/reflections/diego-zamora.md` y `docs/reflections/diego-perez.md` | Pendiente |
| 8 | Presentación de 12 a 15 minutos | Los dos | Estructura de la guía: problema, pregunta, datos, arquitectura, desafío técnico, procesamiento, resultados, limitaciones, conclusión | Pendiente |
| 9 | Comprobar desde una instalación limpia | Los dos | Clonar, `pip install -r requirements.txt`, `pytest`, `python main.py --muestra` | Probado en local (≈1 min) |
| 10 | Entregar | Diego Zamora | Link del repo, link del folder de Databricks, HTML del notebook de respaldo y el heatmap como entregable aparte | Pendiente |

## Checklist contra la guía

### Examen final

| La guía pide | Dónde está | Estado |
|---|---|---|
| Pipeline completo con instrucciones claras | `python main.py`; los cuatro comandos por etapa en el README | Listo |
| `python -m src.analysis.run_analysis` | `src/analysis/run_analysis.py` | Listo |
| Dataset procesado | `data/processed/` (local) y Volume + tablas Delta (Databricks) | Listo |
| Análisis final en `src/analysis/` | `indice_inversion.py`, `modelo_potencial.py`, `mapa_potencial.py`, `graficas.py` | Listo |
| Notebooks de análisis y resultados | `04_indice_databricks.ipynb`, `05_modelo_potencial_databricks.ipynb` (tabla de equivalencias en el README) | Listo |
| Responder pregunta principal y secundarias, patrones, anomalías, implicaciones | `docs/final_report.md` | Listo |
| Resultados: tablas, visualizaciones, métricas, comparaciones | `reports/tables/`, `reports/figures/`, métricas del modelo contra referencias (`modelo_metricas.csv`) | Listo |
| Entregable extra: heatmap interactivo por metrópoli (clic = números e insights) | `reports/figures/mapa_potencial.html`, aparte del notebook; también en `entrega_final.zip` | Listo |
| Reporte final con la estructura sugerida | `docs/final_report.md` | Listo |
| README como puerta de entrada | `README.md` con todas las secciones de la guía | Listo |
| Reproducibilidad desde instalación limpia, sin rutas personales ni contraseñas | `requirements.txt`, `pyproject.toml`, `python main.py --muestra`, rutas desde `src/utils/rutas.py` | Listo |
| Presentación (12–15 min, todos participan) | — | Pendiente |
| Reflexión individual | `docs/reflections/` (plantillas) | Pendiente: cada quien escribe la suya |

### Lo que se conserva de las entregas anteriores

| La guía pide | Dónde está |
|---|---|
| README, alcance, "¿qué representa una fila?" | `README.md`, `docs/project_scope.md` |
| Fuente documentada, muestra en `data/sample/` | README (Fuentes), `data/sample/README.md` |
| Diccionario de datos | `docs/data_dictionary.md` |
| Exploración inicial | `notebooks/entregable_1.ipynb` |
| Arquitectura actualizada | `docs/architecture.md` (arquitectura final, capas, modelo, colaboración) |
| `download_data` sin credenciales, con caché, errores y trazabilidad | `src/ingestion/download_data.py`, manifiesto sha256 |
| Capas raw → interim → processed | `src/processing/build_dataset.py` |
| Validaciones (8 categorías) | `src/validation/`, `docs/validation.md` |
| Comparación de desempeño | `src/processing/benchmark.py`, `docs/desempeno.md` |
| Decisiones técnicas | `docs/decisions.md` |
| Pruebas | `tests/` (68 pruebas) |
| Issues, ramas, PR revisados, integración | GitHub: 13 PR con merge en `main`, cada uno aprobado por el otro integrante; ramas `1-estructura` a `7-readme-notebook` conservadas |

### Participación individual

| Criterio | Diego Zamora | Diego Pérez |
|---|---|---|
| Commits relevantes | Sí (autor de los PR técnicos) | Sí (README, hallazgos, archivos del E1; coautor en los commits del final) |
| Pull requests propios | Sí | Sí (PR #14 de documentación) |
| Revisiones de PR | Sí (PR #14) | Sí (todos los demás) |
| Issues asignados y terminados | Sí | Revisor de cada issue |
| Contribución de documentación o validación | Sí | Sí (README, hallazgos, revisiones) |
| Reflexión individual | Pendiente | Pendiente |
