"""
Analisis final: indice de inversion, modelo de potencial y rentabilidad
(escenario simulado), en orden.

Ejecucion desde la raiz del repositorio (despues de build_dataset y
validate_dataset):

    python -m src.analysis.run_analysis

Pipeline:
    indice_inversion (Gold) -> modelo_potencial (XGBoost + potencial US$ + HTML)
    -> rentabilidad (XGBoost por segmento + supuestos + Excel)
"""

import sys

from src.analysis import indice_inversion, modelo_potencial, rentabilidad


def main():
    for etapa in (indice_inversion.main, modelo_potencial.main, rentabilidad.main):
        codigo = etapa()
        if codigo:
            return codigo
    return 0


if __name__ == "__main__":
    sys.exit(main())
