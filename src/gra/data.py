# Imports
import warnings
warnings.filterwarnings("ignore", "Wswiglal-redir-stdio")
from gwosc.datasets import find_datasets, event_gps, event_detectors
from gwosc.locate import get_event_urls
import gwpy
import gwpy.timeseries
from lalframe.utils import frtools
import os
# Typer apps:
import typer
from rich.console import Console
# Add console
console = Console()

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

def list_data_lvk():
    """ List available data files. """
    events = _list_lvk_data()
    for event in events:
        typer.echo(event)
    typer.echo(f"\nTotal unique events: {len(events)}")
    return events

def _get_lvk_strain_individual(event_name, return_data=False):
    events = check_event_name(event_name)
    gps = event_gps(event_name)
    typer.echo(f"Event '{event_name}' found with GPS time {gps}.")
    start, end = int(gps - 60*10), int(gps + 60*10)  # 10 minutes before and after
    detectors = event_detectors(event_name)
    data = {}
    # Make directory for event if it doesn't exist
    if not os.path.exists(event_name):
        os.makedirs(event_name)
    typer.echo(f"Downloading strain data for detectors: {', '.join(detectors)}")
    for det in detectors:
        filename = f"{event_name}/{event_name}_{det}_strain.gwf"
        # If the file exists, just load it instead of downloading again
        if os.path.exists(filename):
            typer.echo(f"File {filename} already exists.")
            if return_data == False:
                continue
            else:
                # Read the channel name:
                #channel_name = gwpy.io.gwf.get_channel_names(filename)[0]  # Assuming only one channel per file
                channel_name = frtools.get_channels(filename)[0] # Assuming only one channel per file
                data[det] = gwpy.timeseries.TimeSeries.read(filename, format='gwf', channel=channel_name)
                typer.echo(f"Data loaded from {filename}.")
                continue
        typer.echo(f"Fetching {det} data from {start} to {end}...")
        try:
            data[det] = gwpy.timeseries.TimeSeries.fetch_open_data(det, start, end, cache=True)
            # Set channel name to {det}:GWOSC-STRAIN 
            data[det].channel = f"{det}:GWOSC-STRAIN"
            data[det].write(filename, format='gwf')
            channel_name = data[det].channel
            typer.echo(f"Saved strain data to {filename} with channel {channel_name}.")
        except Exception as e:
            typer.echo(f"Error fetching data for {event_name} with {det}: {e}")
    if return_data:
        return data
    else:
        return None

def _get_lvk_strain_all():
    events = _list_lvk_data()
    data_all = {}
    for event in events:
        data_all[event] = get_lvk_strain(event)
    return events, data_all

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

    # 1. Define the Catalog and Query
    catalog_name = "fp_xsc"
    query = f"SELECT * FROM {catalog_name}"
    
    # 2. Launch Asynchronous Job
    print("Launching asynchronous job...")
    # Use query_tap for custom ADQL; query_region(spatial='all-sky') 
    # often defaults to sync which fails for large datasets.
    job = Irsa.query_tap(query=query, async_job=True)
    
    # 3. Monitor Status
    while job.get_phase() not in ('COMPLETED', 'ERROR', 'ABORTED'):
        print(f"Job Status: {job.get_phase()}... waiting 10s")
        time.sleep(10)
    
    # 4. Handle Results
    if job.get_phase() == 'COMPLETED':
        data_table = job.to_table()
        data_table.write("2mass_galaxy_full.csv", format="ascii.csv", overwrite=True)
        print(f"Success! Downloaded {len(data_table)} rows to {output_file}.")
    else:
        print(f"Job failed with status: {job.get_phase()}")
    return data_table

def _get_2mass_individual(event_name):
    raise ValueError("Not implemented yet; only `all` is supported")

def get_2mass_data(event_name: str = typer.Argument(..., help="Name of the event to download 2MASS data for; otherwise 'all' to download the full 2MASS galaxy catalog")):
    if event_name == 'all':
        return _get_2mass_all()
    else:
        return _get_2mass_individual(event_name)

def get_pe(event_name: str = typer.Argument(..., help="Name of the event to download PE data for")):
    events = check_event_name(event_name)
    datasets = find_datasets(type='pe', event_name=event_name)





