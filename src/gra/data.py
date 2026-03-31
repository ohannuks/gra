"""
Gravitational-wave research assistant – data layer.

This module is the public data API consumed by ``cli.py``.  It is intentionally
thin: LVK-specific logic lives in ``data_lvk``, and this file adds the 2MASS
galaxy-catalogue helpers alongside re-exports so that call sites never need to
know which sub-module owns a function.

Structure
---------
data.py          – 2MASS helpers + public re-exports (this file)
data_lvk.py      – All LVK strain / PE / processing logic

Public API
----------
    get_lvk_strain(event_name, download_pe, segment_length)  [from data_lvk]
    list_data_lvk()                                          [from data_lvk]
    process_lvk_event(event_name)                            [from data_lvk]
    get_2mass_data(event_name)
"""

import os
import astropy
import typer
from rich.console import Console
from .data_lvk import get_lvk_strain, list_data_lvk, process_lvk_event

console = Console()


def _get_2mass_spectroscopic(return_data=False):
    from astroquery.vizier import Vizier
    from astropy.table import Table

    if not os.path.exists("2mass"):
        os.makedirs("2mass")
    filename = "2mass/2mass_galaxy_catalog_spec.csv"
    if os.path.exists(filename):
        typer.echo(f"File {filename} already exists.")
        if return_data == False:
            return None
        else:
            data_table = Table.read(filename, format="ascii.csv")
            typer.echo(f"Data loaded from {filename}.")
            return data_table

    # Configure the Vizier query; row limit -1 to get the full catalog (~43k sources)
    v = Vizier(catalog="J/ApJS/199/26", columns=["2MRS", "RAJ2000", "DEJ2000", "cz"])
    v.ROW_LIMIT = -1

    typer.echo(f"Downloading 2MRS Catalog... this may take a moment.")
    result = v.get_catalogs("J/ApJS/199/26")

    if result:
        m2rs_table = result[0]
        cz_column = m2rs_table['cz']
        speed_of_light_km_s = astropy.constants.c.to('km/s').value
        m2rs_table['z'] = cz_column / speed_of_light_km_s

        typer.echo(f"Successfully downloaded {len(m2rs_table)} sources.")
        typer.echo(m2rs_table[:10])

        m2rs_table.write(filename, format="ascii.csv", overwrite=True)
        typer.echo(f"Catalog saved to {filename}")
        data_table = m2rs_table
    else:
        typer.echo("No data found.")
        data_table = None

    if return_data == False:
        return None
    else:
        return data_table


def _get_2mass_individual(event_name):
    raise ValueError("Not implemented yet; only `all` is supported")


def get_2mass_data(event_name):
    if event_name == 'spectroscopic':
        return _get_2mass_spectroscopic()
    else:
        return _get_2mass_individual(event_name)

