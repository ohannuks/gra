import typer
from gra.data import data_app

app = typer.Typer()

# This makes 'data' a subcommand group of 'gra'
app.add_typer(data_app, name="data")

def main():
    app()

if __name__ == "__main__":
    main()

