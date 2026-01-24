import typer

data_app = typer.Typer(help="Manage your data.")

from gwosc.datasets import find_datasets


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

@data_app.command("ls")
def list_data():
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

# Make a sub command 'get' that allows for downloading of either the strain data or the PE samples. Should work as `gra data get strain GW150914` or `gra data get pe GW150914`.
get_app = typer.Typer(help="Download data for a specific event.")
data_app.add_typer(get_app, name="get")

@get_app.command("strain")
def get_strain(event_name: str = typer.Argument(..., help="Name of the event to download strain data for")):
    datasets = find_datasets(type='strain', event_name=event_name)
    if not datasets:
        typer.echo(f"No strain datasets found for event '{event_name}'.")
        raise typer.Exit(code=1)
    for dataset in datasets:
        typer.echo(f"Downloading {dataset}...")
        # Actual download logic goes here
        typer.echo(f"Downloaded {dataset}.")

@get_app.command("pe")
def get_pe(event_name: str = typer.Argument(..., help="Name of the event to download PE data for")):
    datasets = find_datasets(type='pe', event_name=event_name)
    if not datasets:
        typer.echo(f"No PE datasets found for event '{event_name}'.")
        raise typer.Exit(code=1)
    for dataset in datasets:
        typer.echo(f"Downloading {dataset}...")
        # Actual download logic goes here
        typer.echo(f"Downloaded {dataset}.")
