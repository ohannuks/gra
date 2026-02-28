# Imports
import warnings
warnings.filterwarnings("ignore", "Wswiglal-redir-stdio")
from gwosc.datasets import find_datasets, event_gps, event_detectors
from zenodo_get import download as zenodo_download
from gwosc.locate import get_event_urls
import gwpy
import gwpy.timeseries
from lalframe.utils import frtools
import astropy
import os
# Typer apps:
import typer
from rich.console import Console
# Add console
console = Console()

pe_zenodo_releases = {}
pe_zenodo_releases['GWTC-2.1-confident'] = 'https://zenodo.org/records/6513631'
pe_zenodo_releases['GWTC-3-confident'] = 'https://zenodo.org/records/5546663'
pe_zenodo_releases['GWTC-4.0'] = 'https://zenodo.org/records/17014085'

def _get_lvk_pe_data(event_name):
    output_dir = f"{event_name}/pe"
    # Create directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # Check if PE data (hdf5) exists already
    if any(fname.endswith('.hdf5') for fname in os.listdir(output_dir)):
        typer.echo(f"PE data for '{event_name}' already exists in {output_dir}. Skipping download.")
        return

    # 1. Identify which catalog contains the event
    catalogs = list(pe_zenodo_releases.keys()) + ['O4_Discovery_Papers']
    catalog = None
    
    for cat in catalogs:
        # find_datasets returns a list of event names in that catalog
        available_events = find_datasets(type='event', catalog=cat)
        if any(event_name in event for event in available_events):
            catalog = cat
            break

    if catalog is None:
        typer.echo(f"Event '{event_name}' not found in available catalogs.")
        raise typer.Exit(code=1)

    # 2. Extract Record ID and download from Zenodo
    if catalog in pe_zenodo_releases:
        zenodo_url = pe_zenodo_releases[catalog]
        # Extract the numeric record ID from the end of the URL
        record_id = zenodo_url.split('/')[-1]
        
        typer.echo(f"Downloading PE data for '{event_name}' from Zenodo record {record_id}...")
        
        # Download files matching the event name (e.g., HDF5 posterior samples)
        zenodo_download(
            record_id, 
            output_dir=f"{event_name}/pe", 
            file_glob=f"*{event_name}*" 
        )
        typer.echo(f"Download complete. Files saved to: {output_dir}")
    else:
        typer.echo(f"No Zenodo release mapping found for catalog '{catalog}'.")

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
    ''' Download strain data for a specific event. '''
    info = _get_lvk_info_individual(event_name)
    events = info['event_name']
    gps = info['gps']
    detectors = info['detectors']
    typer.echo(f"Event '{event_name}' found with GPS time {gps}.")
    start, end = int(gps - 60*10), int(gps + 60*10)  # 10 minutes before and after
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
    # Download also PE data:
    _get_lvk_pe_data(event_name)
    if return_data:
        return data
    else:
        return None

def _get_lvk_info_individual(event_name):
    ''' Download info data for a specific event and save in the same folder as the strain data. 
    '''
    # Make directory for event if it doesn't exist
    if not os.path.exists(event_name):
        os.makedirs(event_name)
    filename = f"{event_name}/{event_name}_info.json"
    # If the file exists, just load it instead of downloading again
    if os.path.exists(filename):
        typer.echo(f"File {filename} already exists.")
        with open(filename, 'r') as f:
            import json
            info = json.load(f)
            info['detectors'] = set(info['detectors'])  # Convert back to set
        typer.echo(f"Data loaded from {filename}.")
        return info
    events = check_event_name(event_name)
    gps = event_gps(event_name)
    detectors = event_detectors(event_name)
    info = {
        'event_name': event_name,
        'gps': gps,
        'detectors': list(detectors)
    }
    with open(filename, 'w') as f:
        import json
        json.dump(info, f, indent=4)
    typer.echo(f"Saved event info to {filename}.")
    return info

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

def _get_2mass_spectroscopic(return_data=False):
    from astroquery.vizier import Vizier
    from astropy.table import Table
 
    # Create a folder if it doesn't exist
    if not os.path.exists("2mass"):
        os.makedirs("2mass")
    filename = "2mass/2mass_galaxy_catalog_spec.csv"
    # If the file exists, just load it instead of downloading again
    if os.path.exists(filename):
        typer.echo(f"File {filename} already exists.")
        if return_data == False:
            return None
        else:
            data_table = Table.read(filename, format="ascii.csv")
            typer.echo(f"Data loaded from {filename}.")
            return data_table

    # 1. Configure the Vizier query
    # We set the row limit to -1 to get the full catalog (~43k sources)
    v = Vizier(catalog="J/ApJS/199/26", columns=["2MRS", "RAJ2000", "DEJ2000", "cz"])
    v.ROW_LIMIT = -1
    
    # 2. Fetch the catalog
    typer.echo(f"Downloading 2MRS Catalog... this may take a moment.")
    result = v.get_catalogs("J/ApJS/199/26")
    
    # 3. Access the primary table (typically the first index)
    if result:
        m2rs_table = result[0]
        # Add z column from cz (redshift = velocity / speed of light)
        cz_column = m2rs_table['cz']
        speed_of_light_km_s = astropy.constants.c.to('km/s').value
        m2rs_table['z'] = cz_column / speed_of_light_km_s
        
        # 4. Display summary
        typer.echo(f"Successfully downloaded {len(m2rs_table)} sources.")
        typer.echo(m2rs_table[:10])  # Show first 10 rows
        
        # Save to a local CSV file:
        m2rs_table.write(filename, format="ascii.csv", overwrite=True)
        typer.echo(f"Catalog saved to {filename}")
        data_table = m2rs_table
    else:
        typer.echo("No data found.")
        data_table = None
    if return_data == False:
        return None
    else:
        return data_table

def _get_2mass_individual(event_name):
    raise ValueError("Not implemented yet; only `all` is supported")

def get_2mass_data(event_name: str = typer.Argument(..., help="Name of the event to download 2MASS data for; otherwise 'spectorscopic' to download the spectroscopic 3D 2MASS catalog")):
    if event_name == 'spectroscopic':
        return _get_2mass_spectroscopic()
    else:
        return _get_2mass_individual(event_name)

def get_pe(event_name: str = typer.Argument(..., help="Name of the event to download PE data for")):
    events = check_event_name(event_name)
    datasets = find_datasets(type='pe', event_name=event_name)
