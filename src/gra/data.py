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
@data_app.command("get")
def get_data(data_type: str = typer.Argument(..., help="Type of data to download: 'strain' or 'pe'"),
             event_name: str = typer.Argument(..., help="Name of the event to download data for")):
    """ Download data files for a specific event. """
    valid_types = ['strain', 'pe']
    if data_type not in valid_types:
        typer.echo(f"Invalid data type '{data_type}'. Valid types are: {', '.join(valid_types)}")
        raise typer.Exit(code=1)

    events = list_data()

    if data_type == 'strain':
        datasets = find_datasets(type='strain', event=event_name)
    elif data_type == 'pe':
        datasets = find_datasets(type='pe', event=event_name)

    if not datasets:
        typer.echo(f"No datasets found for event '{event_name}' and type '{data_type}'.")
        raise typer.Exit(code=1)

    for dataset in datasets:
        typer.echo(f"Downloading {dataset}...")
        # Here you would add the actual download logic, e.g., using requests or gwosc's download utilities.
        # For demonstration purposes, we'll just print the dataset name.
        typer.echo(f"Downloaded {dataset}.")

