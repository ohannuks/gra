import typer
# Typer apps:
##############################
app = typer.Typer()
data_app = typer.Typer(help="Manage your data.")

data_get_app = typer.Typer(help="Download data for a specific event.") # Make a sub command 'get' that allows for downloading of either the strain data or the PE samples. Should work as `gra data get strain GW150914` or `gra data get pe GW150914`.
data_inject_app = typer.Typer(help="Create injection data with either real or simulated noise.")
data_ls_app = typer.Typer(help="List available data files.")

data_inject_lvk_app = typer.Typer(help="Create injection data with either real or simulated noise from the LVK catalogs.")

data_process_app = typer.Typer(help="Process data files, e.g. produce the PSD, make figures of the data, etc.")

data_ls_lvk_app = typer.Typer(help="List available data files from the LVK catalogs.")

# Add subcommands to data_app
app.add_typer(data_app, name="data")
# data [get/inject/ls]
data_app.add_typer(data_get_app, name="get")
data_app.add_typer(data_inject_app, name="inject")
data_app.add_typer(data_process_app, name="process")
data_app.add_typer(data_ls_app, name="ls")
##############################

@data_get_app.command("lvk")
def get_lvk_strain(
        event_name: str = typer.Argument(..., help="Name of the event to download strain data for; or 'all' if you want to download for all events"), 
        no_pe: bool = typer.Option(False, "--no-pe", help="Do not download parameter estimation samples"),
        segment_length: int = typer.Option(1200, "--segment-length", help="Total length of the data segment to download in seconds around the trigger time of the event, default is +- 10 minutes (60*20 seconds)"),
        ):
    from . import data
    if no_pe:
        download_pe = False
    else:
        download_pe = True
    return data.get_lvk_strain(event_name, download_pe, segment_length)

@data_get_app.command("2mass")
def get_2mass_data(event_name: str = typer.Argument(..., help="Name of the event to download 2MASS data for")):
    from . import data
    return data.get_2mass_data(event_name)

@data_process_app.command("lvk")
def process_lvk_event(event_name: str = typer.Argument(..., help="Name of the event to process data for")):
    """ Process data for a specific event. """
    from . import data
    return data.process_lvk_event(event_name)

@data_ls_app.command("lvk")
def list_data_lvk():
    """ List available data files. """
    from . import data
    return data.list_data_lvk()

def main():
    app()

if __name__ == "__main__":
    main()

