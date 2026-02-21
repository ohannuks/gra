import typer
# Typer apps:
##############################
app = typer.Typer()
data_app = typer.Typer(help="Manage your data.")

data_get_app = typer.Typer(help="Download data for a specific event.") # Make a sub command 'get' that allows for downloading of either the strain data or the PE samples. Should work as `gra data get strain GW150914` or `gra data get pe GW150914`.
data_inject_app = typer.Typer(help="Create injection data with either real or simulated noise.")
data_ls_app = typer.Typer(help="List available data files.")

#data_get_lvk_app = typer.Typer(help="Download data for a specific or all event from the LVK catalogs.")
#data_get_2mass_app = typer.Typer(help="Download data for a specific or all event from the 2MASS catalog.")

data_inject_lvk_app = typer.Typer(help="Create injection data with either real or simulated noise from the LVK catalogs.")

data_ls_lvk_app = typer.Typer(help="List available data files from the LVK catalogs.")

# Add subcommands to data_app
app.add_typer(data_app, name="data")
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

@data_ls_app.command("lvk")
def list_data_lvk():
    """ List available data files. """
    import data
    return data.list_data_lvk()

@data_get_app.command("lvk")
def get_lvk_strain(event_name: str = typer.Argument(..., help="Name of the event to download strain data for; or 'all' if you want to download for all events")):
    import data
    return data.get_lvk_strain(event_name)

@data_get_app.command("2mass")
def get_2mass_data(event_name: str = typer.Argument(..., help="Name of the event to download 2MASS data for")):
    import data
    return data.get_2mass_data(event_name)

def main():
    app()

if __name__ == "__main__":
    main()

