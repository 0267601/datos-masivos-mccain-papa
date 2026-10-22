"""
Muestra cruda para correr el pipeline sin internet.

Reconstruimos data/raw/ a partir de las copias de respaldo del entregable 1
(data/siap_papa_2013_2025.csv y data/sniim_papa_2025.csv), en el mismo
formato en que las entregan las fuentes. Asi el pipeline ejercita los mismos
problemas que con la descarga real:

    SIAP  -> un CSV por anio, latin-1, con el schema drift de los encabezados:
             hasta 2015 "Precio"; 2016-2020 "Nomcultivo Sin Um" y "Precio";
             desde 2021 "Nomcultivo" y "Preciomediorural".
    SNIIM -> un HTML por variedad y mes con table#tblResultados, dos filas de
             encabezado y celdas td.Datos2.

Diferencias con la descarga real: el SIAP real trae todos los cultivos (aqui
solo papa) y el HTML real trae ademas las tablas de diseno del portal.

Pipeline:
    CSV del E1 -> formato crudo de cada fuente -> data/raw/
"""

import html

import pandas as pd

from src.ingestion.sniim_extractor import VARIEDADES_PAPA
from src.utils.rutas import DATA, RAW

COPIA_SIAP = DATA / "siap_papa_2013_2025.csv"
COPIA_SNIIM = DATA / "sniim_papa_2025.csv"

# Meses del SNIIM que dejamos en la muestra: el anio completo pesa mas sin
# aportar nada nuevo a las pruebas del pipeline.
MESES_MUESTRA = [1, 2, 3]

ENCABEZADO_SNIIM = (
    "<tr>" + "".join(f'<td class="encabTabla">{c}</td>' for c in
                     ["Fecha", "Presentación", "Origen", "Destino", "Precio Mín",
                      "Precio Max", "Precio Frec", "Obs."]) + "</tr>\n"
    '<tr><td class="encabTabla" colspan="8">Precios por kilogramo</td></tr>\n'
)


def encabezados_siap(anio):
    """Devolvemos los renombres que tenia el portal en ese anio."""
    if anio <= 2015:
        return {"Preciomediorural": "Precio"}
    if anio <= 2020:
        return {"Nomcultivo": "Nomcultivo Sin Um", "Preciomediorural": "Precio"}
    return {}


def escribir_siap():
    siap = pd.read_csv(COPIA_SIAP)
    carpeta = RAW / "siap"
    carpeta.mkdir(parents=True, exist_ok=True)

    for anio, grupo in siap.groupby("Anio"):
        (grupo.rename(columns=encabezados_siap(anio))
              .to_csv(carpeta / f"siap_cierre_{anio}.csv", index=False, encoding="latin-1"))

    return siap.Anio.nunique()


def pagina_sniim(variedad, registros):
    """Armamos la pagina de resultados con la misma estructura que el portal."""
    filas = []
    for r in registros.itertuples():
        celdas = [r.fecha.strftime("%d/%m/%Y"), r.presentacion, r.origen, r.destino,
                  f"{r.precio_min:.2f}", f"{r.precio_max:.2f}", f"{r.precio_frec:.2f}",
                  r.obs]
        # La primera celda del portal trae colspan="1"; la conservamos.
        atributos = [' colspan="1"'] + [""] * (len(celdas) - 1)
        tds = "".join(
            f'<td class="Datos2"{extra}>{html.escape(str(c))}</td>'
            for extra, c in zip(atributos, celdas)
        )
        filas.append(f"<tr>{tds}</tr>")

    return (
        '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">\n'
        "<HTML><HEAD><title>ResultadosConsultaFechaFrutasYHortalizas</title></HEAD><body>\n"
        f'<table id="tblDatosGenerales"><tr><td class="titDatos">Papa {variedad}</td></tr></table>\n'
        '<table id="tblResultados">\n' + ENCABEZADO_SNIIM + "\n".join(filas) +
        "\n</table></body></HTML>\n"
    )


def escribir_sniim():
    sniim = pd.read_csv(COPIA_SNIIM, keep_default_na=False,
                        dtype={"obs": str, "presentacion": str, "origen": str, "destino": str})
    sniim["fecha"] = pd.to_datetime(sniim["fecha"])
    sniim = sniim[sniim["fecha"].dt.month.isin(MESES_MUESTRA)]

    ids = {nombre: pid for pid, nombre in VARIEDADES_PAPA.items()}
    paginas = 0

    for (variedad, mes), grupo in sniim.groupby(["variedad", sniim["fecha"].dt.month]):
        anio = int(grupo["fecha"].dt.year.iloc[0])
        carpeta = RAW / "sniim" / variedad.lower().replace(" ", "_")
        carpeta.mkdir(parents=True, exist_ok=True)
        ruta = carpeta / f"sniim_{ids[variedad]}_{anio}-{mes:02d}.html"
        ruta.write_text(pagina_sniim(variedad, grupo), encoding="utf-8")
        paginas += 1

    return paginas


def construir_muestra():
    """Escribimos la muestra en data/raw/ y devolvemos cuantos archivos generamos."""
    return {"siap": escribir_siap(), "sniim": escribir_sniim()}
