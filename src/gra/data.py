# Imports
import warnings
warnings.filterwarnings("ignore", "Wswiglal-redir-stdio")
from gwosc.datasets import find_datasets, event_gps, event_detectors
from gwosc.locate import get_event_urls
import gwpy
import gwpy.timeseries
import os
# Typer apps:
import typer

# Typer apps:
##############################
data_app = typer.Typer(help="Manage your data.")

data_get_app = typer.Typer(help="Download data for a specific event.") # Make a sub command 'get' that allows for downloading of either the strain data or the PE samples. Should work as `gra data get strain GW150914` or `gra data get pe GW150914`.
data_inject_app = typer.Typer(help="Create injection data with either real or simulated noise.")
data_ls_app = typer.Typer(help="List available data files.")

#data_get_lvk_app = typer.Typer(help="Download data for a specific or all event from the LVK catalogs.")
#data_get_2mass_app = typer.Typer(help="Download data for a specific or all event from the 2MASS catalog.")

data_inject_lvk_app = typer.Typer(help="Create injection data with either real or simulated noise from the LVK catalogs.")

data_ls_lvk_app = typer.Typer(help="List available data files from the LVK catalogs.")

# Add subcommands to data_app
# data [get/inject/ls]
data_app.add_typer(data_get_app, name="get")
data_app.add_typer(data_inject_app, name="inject")
data_app.add_typer(data_ls_app, name="ls")
# data get [lvk/2mass]
#data_get_app.add_typer(data_get_lvk_app, name="lvk")
#data_get_app.add_typer(data_get_2mass_app, name="2mass")
# data inject [lvk]
data_inject_app.add_typer(data_inject_lvk_app, name="lvk")
##############################

def remove_duplicates(seq):
    # Each event has multiple versions, so the last bit in the name of '-vX'. We remove it and then remove duplicates.
    seen = set()
    result = []
    for item in seq:
        item_base = item.rsplit('-v', 1)[0]  # Remove version suffix
        if item_base not in seen:
            seen.add(item_base)
            result.append(item_base)
    return result

def _list_lvk_data():
    """ List available data files. """
    catalogs = ['GWTC-1-confident', 
                'GWTC-2.1-confident', 
                'GWTC-3-confident',
                'GWTC-4.0',
                'O4_Discovery_Papers']
    events_all = []
    for catalog in catalogs:
        events = find_datasets(type='events', catalog=catalog)
        events = remove_duplicates(events)
        events_all.extend(events)
    return events_all

def check_event_name(event_name):
    events = _list_lvk_data()
    if event_name not in events:
        typer.echo(f"Event '{event_name}' not found in available events.")
        raise typer.Exit(code=1)
    return events

@data_ls_app.command("lvk")
def list_data_lvk():
    """ List available data files. """
    events = _list_lvk_data()
    for event in events:
        typer.echo(event)
    typer.echo(f"\nTotal unique events: {len(events)}")
    return events

def _get_lvk_strain_individual(event_name):
    events = check_event_name(event_name)
    gps = event_gps(event_name)
    typer.echo(f"Event '{event_name}' found with GPS time {gps}.")
    start, end = int(gps - 60*10), int(gps + 60*10)  # 10 minutes before and after
    detectors = event_detectors(event_name)
    data = {}
    typer.echo(f"Downloading strain data for detectors: {', '.join(detectors)}")
    for det in detectors:
        typer.echo(f"Fetching {det} data from {start} to {end}...")
        data[det] = gwpy.timeseries.TimeSeries.fetch_open_data(det, start, end, cache=True)
        # Make directory for event if it doesn't exist
        if not os.path.exists(event_name):
            os.makedirs(event_name)
        filename = f"{event_name}/{event_name}_{det}_strain.gwf"
        data[det].write(filename, format='gwf')
        typer.echo(f"Saved strain data to {filename}.")
    return data

def _get_lvk_strain_all():
    events = _list_lvk_data()
    data_all = {}
    for event in events:
        data_all[event] = get_lvk_strain(event)
    return events, data_all

@data_get_app.command("lvk")
def get_lvk_strain(event_name: str = typer.Argument(..., help="Name of the event to download strain data for; or 'all' if you want to download for all events")):
    if event_name == 'all':
        return _get_lvk_strain_all()
    else:
        return _get_lvk_strain_individual(event_name)

def _get_2mass_all():
    from astroquery.ipac.irsa import Irsa
    import astropy.units as u
    
    # Create a folder if it doesn't exist
    if not os.path.exists("2mass"):
        os.makedirs("2mass")
    output_file = "2mass/2mass_galaxy_catalog.csv"
    # If the file exists, just load it instead of downloading again
    if os.path.exists(output_file):
        print(f"File {output_file} already exists. Loading data from file...")
        from astropy.table import Table
        data_table = Table.read(output_file, format="ascii.csv")
        print(f"Data loaded from {output_file}. Total sources: {len(data_table)}")
        return data_table

    # 1. Identify the correct catalog name
    # The 2MASS Extended Source Catalog is typically 'fp_xsc'
    catalog_name = "fp_xsc"
    
    # 2. Perform an all-sky query
    # 'select *' retrieves all columns. You can specify columns to reduce file size.
    print(f"Starting download of {catalog_name}...")
    data_table = Irsa.query_region(
        catalog=catalog_name, 
        spatial="all-sky"
    )
    
    # 3. Save the data to a local file (e.g., CSV or FITS)
    # Download if the file doesn't exist, otherwise load from file
    data_table.write(output_file, format="ascii.csv", overwrite=True)
    
    print(f"Download complete. Saved to {output_file}")
    print(f"Total sources downloaded: {len(data_table)}")
    return data_table

def _get_2mass_individual(event_name):
    raise ValueError("Not implemented yet; only `all` is supported")

@data_get_app.command("2mass")
def get_2mass_data(event_name: str = typer.Argument(..., help="Name of the event to download 2MASS data for")):
    if event_name == 'all':
        return _get_2mass_all()
    else:
        return _get_2mass_individual(event_name)

def get_pe(event_name: str = typer.Argument(..., help="Name of the event to download PE data for")):
    events = check_event_name(event_name)
    datasets = find_datasets(type='pe', event_name=event_name)





