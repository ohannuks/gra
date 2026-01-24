import typer

data_app = typer.Typer(help="Manage your data.")

@data_app.command("ls")
def list_data():
    """List available data files."""
    print("GW170817, GW150914, GW190412")


