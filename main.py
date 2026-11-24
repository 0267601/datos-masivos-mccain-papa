"""
Comando principal del proyecto: corre el pipeline completo.

    python main.py                  # descarga, procesa, valida y analiza
    python main.py --sin-descarga   # usa lo que ya esta en data/raw
    python main.py --help           # todas las opciones

Ver src/pipeline.py.
"""

import sys

from src.pipeline import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
