"""
Fixtures compartidas de las pruebas.

Las pruebas no tocan internet ni data/: armamos datos crudos diminutos en una
carpeta temporal, con los mismos problemas que encontramos en las fuentes
reales (encabezados que cambian, latin-1, numeros como texto, duplicados).
"""

import pytest

from src.utils.spark import crear_spark

# Una fila de papa con los encabezados de 2013 ("Precio") y otra de 2017
# ("Nomcultivo Sin Um"), mas un cultivo que no es papa y un duplicado.
SIAP_2013 = (
    "Anio,Nomestado,Nomddr,Nomcader,Nommunicipio,Nomcicloproductivo,Nommodalidad,"
    "Nomcultivo,Sembrada,Cosechada,Siniestrada,Volumenproduccion,Rendimiento,Precio,Valorproduccion\n"
    "2013,Sonora,Navojoa,Navojoa,Navojoa,Otoño-Invierno,Riego,Papa,100,100,0,\"3,000\",30,5000,15000000\n"
    "2013,Sonora,Navojoa,Navojoa,Navojoa,Otoño-Invierno,Riego,Papa,100,100,0,\"3,000\",30,5000,15000000\n"
    "2013,Sonora,Navojoa,Navojoa,Navojoa,Otoño-Invierno,Riego,Maíz grano,50,50,0,500,10,4000,2000000\n"
    "2013,Distrito Federal,Xochimilco,Xochimilco,Xochimilco,Primavera-Verano,Temporal,Papa,10,8,2,80,10,7000,560000\n"
)
SIAP_2017 = (
    "Anio,Nomestado,Nomddr,Nomcader,Nommunicipio,Nomcicloproductivo,Nommodalidad,"
    "Nomcultivo Sin Um,Sembrada,Cosechada,Siniestrada,Volumenproduccion,Rendimiento,Precio,Valorproduccion\n"
    "2017,Sinaloa,Los Mochis,Ahome,Ahome,Otoño-Invierno,Riego,Papa,200,200,0,6000,30,6000,36000000\n"
)


def html_sniim(filas):
    """Armamos una pagina con la estructura de tblResultados del SNIIM."""
    cuerpo = "".join(
        "<tr>" + "".join(f'<td class="Datos2">{c}</td>' for c in fila) + "</tr>"
        for fila in filas
    )
    return (
        "<html><body><table id='otra'><tr><td>diseno</td></tr></table>"
        "<table id='tblResultados'>"
        "<tr><td>Fecha</td><td>Presentación</td><td>Origen</td><td>Destino</td>"
        "<td>Precio Mín</td><td>Precio Max</td><td>Precio Frec</td><td>Obs.</td></tr>"
        "<tr><td colspan='8'>Precios por kilogramo</td></tr>"
        f"{cuerpo}</table></body></html>"
    )


FILAS_ALPHA = [
    ["03/03/2025", "Kilogramo", "Sinaloa", "DF: Central de Abasto de Iztapalapa DF",
     "18.00", "22.00", "20.00", ""],
    ["03/03/2025", "Kilogramo", "Sinaloa", "DF: Central de Abasto de Iztapalapa DF",
     "18.00", "22.00", "20.00", ""],                       # duplicado
    ["04/03/2025", "Kilogramo", "Distrito Federal", "Jalisco: Mercado de Abasto",
     "19.00", "23.00", "21.00", ""],
]
FILAS_ROUSET = [
    ["05/03/2025", "Arpilla de 22 kg.", "Importación",
     "Baja California : Central de Abasto INDIA, Tijuana", "9.00", "11.00", "10.00", ""],
]


@pytest.fixture(scope="session")
def spark():
    sesion = crear_spark("DatosMasivos-Pruebas", particiones_shuffle=2, memoria="1g")
    yield sesion
    sesion.stop()


@pytest.fixture
def raw_siap(tmp_path):
    carpeta = tmp_path / "raw" / "siap"
    carpeta.mkdir(parents=True)
    (carpeta / "siap_cierre_2013.csv").write_bytes(SIAP_2013.encode("latin-1"))
    (carpeta / "siap_cierre_2017.csv").write_bytes(SIAP_2017.encode("latin-1"))
    return sorted(carpeta.glob("*.csv"))


@pytest.fixture
def raw_sniim(tmp_path):
    carpeta = tmp_path / "raw" / "sniim"
    (carpeta / "alpha").mkdir(parents=True)
    (carpeta / "rouset").mkdir(parents=True)
    (carpeta / "alpha" / "sniim_740_2025-03.html").write_text(html_sniim(FILAS_ALPHA), "utf-8")
    (carpeta / "rouset" / "sniim_766_2025-03.html").write_text(html_sniim(FILAS_ROUSET), "utf-8")
    return carpeta


VARIEDADES = {740: "Alpha", 766: "Rouset"}
