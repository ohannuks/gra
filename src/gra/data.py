# Imports
import warnings
warnings.filterwarnings("ignore", "Wswiglal-redir-stdio")
from gwosc.datasets import find_datasets, event_gps, event_detectors
from gwosc.locate import get_event_urls
import gwpy
import gwpy.timeseries
# Typer apps:
import typer

# Typer apps:
###
data_app = typer.Typer(help="Manage your data.")
data_get_app = typer.Typer(help="Download data for a specific event.") # Make a sub command 'get' that allows for downloading of either the strain data or the PE samples. Should work as `gra data get strain GW150914` or `gra data get pe GW150914`.
data_inject_app = typer.Typer(help="Create injection data with either real or simulated noise.")
# Add subcommands to data_app
data_app.add_typer(data_get_app, name="get")
data_app.add_typer(data_inject_app, name="inject")
###

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

def _list_data():
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
    events = _list_data()
    if event_name not in events:
        typer.echo(f"Event '{event_name}' not found in available events.")
        raise typer.Exit(code=1)
    return events

@data_app.command("ls")
def list_data():
    """ List available data files. """
    events = _list_data()
    for event in events:
        typer.echo(event)
    typer.echo(f"\nTotal unique events: {len(events)}")
    return events

@data_get_app.command("strain")
def get_strain(event_name: str = typer.Argument(..., help="Name of the event to download strain data for")):
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
        filename = f"{event_name}_{det}_strain.gwf"
        data[det].write(filename, format='gwf')
        typer.echo(f"Saved strain data to {filename}.")
    return data

@data_get_app.command("pe")
def get_pe(event_name: str = typer.Argument(..., help="Name of the event to download PE data for")):
    events = check_event_name(event_name)
    datasets = find_datasets(type='pe', event_name=event_name)





