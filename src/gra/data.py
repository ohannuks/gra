import typer
data_app = typer.Typer(help="Manage your data.")
data_get_app = typer.Typer(help="Download data for a specific event.") # Make a sub command 'get' that allows for downloading of either the strain data or the PE samples. Should work as `gra data get strain GW150914` or `gra data get pe GW150914`.
data_inject_app = typer.Typer(help="Create injection data with either real or simulated noise.")
# Add subcommands to data_app
data_app.add_typer(data_get_app, name="get")
data_app.add_typer(data_inject_app, name="inject")

# Imports
import warnings
warnings.filterwarnings("ignore", "Wswiglal-redir-stdio")
from gwosc.datasets import find_datasets, event_gps
import gwpy

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
                'GWTC-4.0']
    events_all = []
    for catalog in catalogs:
        events = find_datasets(type='events', catalog=catalog)
        events = remove_duplicates(events)
        for event in events:
            typer.echo(event)
        events_all.extend(events)
    typer.echo(f"\nTotal unique events: {len(set(events_all))}")
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
    return _list_data()

@data_get_app.command("strain")
def get_strain(event_name: str = typer.Argument(..., help="Name of the event to download strain data for")):
    events = check_event_name(event_name)
    datasets = find_datasets(type='strain', event_name=event_name)

@data_get_app.command("pe")
def get_pe(event_name: str = typer.Argument(..., help="Name of the event to download PE data for")):
    events = check_event_name(event_name)
    datasets = find_datasets(type='pe', event_name=event_name)





