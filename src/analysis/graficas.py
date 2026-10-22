"""
Graficas del indice de inversion.

Tres graficas, una por pregunta:
    1. ranking_metropolis      ¿donde invertir?   top 15 por indice, con lo que
                                                  aporta cada componente
    2. competencia_importacion ¿contra quien?     de donde vienen las papas
                                                  importadas y a que precio
    3. mapa_oportunidad        ¿por que ahi?      crecimiento contra densidad
                                                  de restaurantes

Trabajamos con tablas ya agregadas (decenas de filas), asi que aqui si usamos
pandas y matplotlib. Las funciones reciben DataFrames de pandas y regresan la
figura: el notebook de Databricks las muestra y main() las guarda.

Colores: paleta categorica validada para daltonismo (azul, naranja, aqua,
amarillo), siempre en el mismo orden y con etiquetas visibles; gris para
"otros" o lo que no se resalta.
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from src.utils.rutas import FIGURAS, PROCESSED  # noqa: E402

AZUL, NARANJA, AQUA, AMARILLO = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
GRIS, GRIS_CLARO = "#8a8984", "#d9d8d4"
TEXTO, TEXTO_SECUNDARIO, FONDO = "#0b0b0b", "#52514e", "#fcfcfb"

COMPONENTES = [  # (columna de puntaje, etiqueta, color) en orden fijo
    ("puntaje_crecimiento", "A. Crecimiento de restaurantes", AZUL),
    ("puntaje_cadenas", "B. Aperturas de cadenas", NARANJA),
    ("puntaje_turismo", "Turismo (hoteles)", AQUA),
    ("puntaje_mercado", "Tamaño de mercado", AMARILLO),
]
ORIGENES = [("Estados Unidos", AZUL), ("Canada", NARANJA), ("Belgica", AQUA)]


def _estilo(ax):
    ax.set_facecolor(FONDO)
    for lado in ("top", "right"):
        ax.spines[lado].set_visible(False)
    for lado in ("left", "bottom"):
        ax.spines[lado].set_color(GRIS_CLARO)
    ax.tick_params(colors=TEXTO_SECUNDARIO, labelsize=9)
    ax.grid(axis="x", color=GRIS_CLARO, linewidth=0.6, alpha=0.7)
    ax.set_axisbelow(True)


def ranking_metropolis(indice, pesos, top=15):
    """Barras apiladas: cuanto aporta cada componente al indice del top 15."""
    datos = indice.sort_values("indice", ascending=False).head(top).iloc[::-1]
    total_pesos = sum(pesos.values())

    fig, ax = plt.subplots(figsize=(9, 6.5), facecolor=FONDO)
    izquierda = pd.Series(0.0, index=datos.index)
    for columna, etiqueta, color in COMPONENTES:
        clave = columna.replace("puntaje_", "")
        aporte = datos[columna].fillna(0) * pesos[clave] / total_pesos
        ax.barh(datos["metropoli"], aporte, left=izquierda, color=color, label=etiqueta,
                height=0.62, edgecolor=FONDO, linewidth=1.5)
        izquierda += aporte

    for y, (_, fila) in enumerate(datos.iterrows()):
        ax.text(fila["indice"] + 1, y, f"{fila['indice']:.0f}", va="center",
                fontsize=9, color=TEXTO)

    _estilo(ax)
    ax.set_xlim(0, 105)
    ax.set_xlabel("Índice de inversión (0–100)", color=TEXTO_SECUNDARIO, fontsize=9)
    ax.set_title("¿Dónde invertir? Top zonas metropolitanas", loc="left",
                 fontsize=13, color=TEXTO, pad=10)
    ax.legend(ncol=4, fontsize=8, frameon=False, loc="upper center",
              bbox_to_anchor=(0.5, -0.09), labelcolor=TEXTO_SECUNDARIO)
    fig.tight_layout()
    return fig


def competencia_importacion(anual):
    """Dos paneles, mismo eje x: valor importado por origen y precio por kilo."""
    nombres = [n for n, _ in ORIGENES]
    datos = anual.copy()
    datos["origen"] = datos["pais"].where(datos["pais"].isin(nombres), "Otros")
    valor = (datos.groupby(["anio", "origen"])["valor_usd"].sum().unstack(fill_value=0) / 1e6)
    anios = valor.index.astype(int)

    fig, (izq, der) = plt.subplots(1, 2, figsize=(11, 4.4), facecolor=FONDO)

    abajo = pd.Series(0.0, index=valor.index)
    for nombre, color in ORIGENES + [("Otros", GRIS)]:
        if nombre in valor:
            izq.bar(anios, valor[nombre], bottom=abajo, color=color, label=nombre,
                    width=0.7, edgecolor=FONDO, linewidth=1.5)
            abajo += valor[nombre]
    _estilo(izq)
    izq.grid(axis="x", visible=False)
    izq.grid(axis="y", color=GRIS_CLARO, linewidth=0.6, alpha=0.7)
    izq.set_title("Importaciones de papas congeladas (US$ millones)", loc="left",
                  fontsize=11, color=TEXTO)
    izq.legend(fontsize=8, frameon=False, labelcolor=TEXTO_SECUNDARIO, loc="upper left")

    for nombre, color in ORIGENES:
        serie = datos[datos["pais"] == nombre].sort_values("anio")
        if serie.empty:
            continue
        der.plot(serie["anio"], serie["usd_kg"], color=color, linewidth=2, marker="o",
                 markersize=5, label=nombre)
        ultimo = serie.iloc[-1]
        der.text(ultimo["anio"] + 0.2, ultimo["usd_kg"], f"{nombre} {ultimo['usd_kg']:.2f}",
                 fontsize=8, color=TEXTO_SECUNDARIO, va="center")
    _estilo(der)
    der.grid(axis="x", visible=False)
    der.grid(axis="y", color=GRIS_CLARO, linewidth=0.6, alpha=0.7)
    der.set_xlim(anios.min() - 0.5, anios.max() + 2.8)
    der.set_title("Precio por kilo por origen (US$/kg)", loc="left", fontsize=11, color=TEXTO)

    fig.suptitle("¿Contra quién competimos? Origen de las papas importadas", x=0.01,
                 ha="left", fontsize=13, color=TEXTO)
    fig.tight_layout()
    return fig


def _separar_etiquetas(destacadas, datos, separacion=0.045):
    """Empujamos hacia arriba las etiquetas que se enciman con otra cercana.

    Trabajamos en fracciones del rango de cada eje: dos etiquetas chocan si
    estan a menos de 18% en x y de `separacion` en y.
    """
    rango_x = (datos["restaurantes_100k"].max() - datos["restaurantes_100k"].min()) or 1
    rango_y = (datos["crecimiento_anual"].max() - datos["crecimiento_anual"].min()) or 1
    filas = [f for _, f in destacadas.sort_values("crecimiento_anual").iterrows()]
    colocadas = []                                   # (x, y_texto)
    for fila in filas:
        x, y = fila["restaurantes_100k"], fila["crecimiento_anual"]
        for cx, cy in colocadas:
            if abs(cx - x) / rango_x < 0.18 and abs(cy - y) / rango_y < separacion:
                y = cy + separacion * rango_y
        colocadas.append((x, y))
    # El texto empieza a la derecha de la burbuja, no encima.
    return [(f, x + 0.03 * rango_x, y) for f, (x, y) in zip(filas, colocadas)]


def mapa_oportunidad(indice, top=10):
    """Crecimiento contra densidad; arriba a la izquierda = crece y aun hay espacio."""
    datos = indice.dropna(subset=["crecimiento_anual", "restaurantes_100k"])
    destacadas = datos.nsmallest(top, "ranking")
    resto = datos.drop(destacadas.index)
    escala = 900 / datos["poblacion_2020"].max()

    fig, ax = plt.subplots(figsize=(9, 6), facecolor=FONDO)
    ax.scatter(resto["restaurantes_100k"], resto["crecimiento_anual"],
               s=resto["poblacion_2020"] * escala + 12, color=GRIS_CLARO,
               edgecolor=FONDO, linewidth=1.5, label="Otras metrópolis")
    ax.scatter(destacadas["restaurantes_100k"], destacadas["crecimiento_anual"],
               s=destacadas["poblacion_2020"] * escala + 12, color=AZUL, alpha=0.85,
               edgecolor=FONDO, linewidth=1.5, label=f"Top {top} del índice")
    for fila, x_texto, y_texto in _separar_etiquetas(destacadas, datos):
        x, y = fila["restaurantes_100k"], fila["crecimiento_anual"]
        ax.annotate(fila["metropoli"], (x, y), xytext=(x_texto, y_texto), textcoords="data",
                    fontsize=8, color=TEXTO, va="center",
                    arrowprops={"arrowstyle": "-", "color": GRIS, "linewidth": 0.6,
                                "shrinkA": 0, "shrinkB": 6})

    ax.axhline(datos["crecimiento_anual"].median(), color=GRIS, linewidth=0.8, linestyle="--")
    ax.axvline(datos["restaurantes_100k"].median(), color=GRIS, linewidth=0.8, linestyle="--")
    _estilo(ax)
    ax.grid(visible=False)
    ax.set_xlabel("Restaurantes objetivo por 100 mil habitantes (hoy)",
                  color=TEXTO_SECUNDARIO, fontsize=9)
    ax.set_ylabel("Crecimiento anual de restaurantes objetivo (%)",
                  color=TEXTO_SECUNDARIO, fontsize=9)
    ax.set_title("¿Por qué ahí? Crecimiento contra densidad (tamaño = población)",
                 loc="left", fontsize=13, color=TEXTO)
    ax.text(0.01, 0.98, "Crece rápido y aún hay espacio", transform=ax.transAxes,
            fontsize=8, color=TEXTO_SECUNDARIO, va="top")
    ax.legend(fontsize=8, frameon=False, loc="lower right", labelcolor=TEXTO_SECUNDARIO,
              markerscale=0.4)
    fig.tight_layout()
    return fig


def generar_todas(spark, pesos=None):
    """Leemos las tablas procesadas, generamos las tres graficas y las guardamos."""
    from src.utils.config import cargar_config

    pesos = pesos or cargar_config()["indice"]["pesos"]
    FIGURAS.mkdir(parents=True, exist_ok=True)
    figuras = {}

    indice = spark.read.parquet(str(PROCESSED / "indice_inversion")).toPandas()
    figuras["ranking_metropolis"] = ranking_metropolis(indice, pesos)
    figuras["mapa_oportunidad"] = mapa_oportunidad(indice)

    ruta_anual = PROCESSED / "importaciones_anuales"
    if ruta_anual.exists():
        anual = spark.read.parquet(str(ruta_anual)).toPandas()
        figuras["competencia_importacion"] = competencia_importacion(anual)

    for nombre, figura in figuras.items():
        figura.savefig(FIGURAS / f"{nombre}.png", dpi=150, facecolor=FONDO)
    return figuras
