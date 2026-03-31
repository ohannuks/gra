"""
LVK (LIGO-Virgo-KAGRA) gravitational-wave data utilities.

This module handles all interactions with LVK open data:

- Strain download (single event or all events in parallel via asyncio +
  ProcessPoolExecutor), backed by GWOSC ``gwpy`` / ``fetch_open_data``.
- Parameter-estimation (PE) posterior sample download from Zenodo records
  mapped in ``pe_zenodo_releases``.
- Event metadata retrieval (GPS time, detector list) cached as JSON.
- HDF5 PE file loading with automatic waveform-approximant selection.
- Post-processing helpers: strain time-series figures and official PSD
  extraction from PE samples.

Public API (re-exported via ``data.py``):
    get_lvk_strain(event_name, download_pe, segment_length)
    list_data_lvk()
    process_lvk_event(event_name)
"""

# Get rid of annoying warning about swiglal redir stdio:
import warnings
warnings.filterwarnings("ignore", "Wswiglal-redir-stdio")

import numpy as np
import h5py
import gwpy
from gwosc.datasets import find_datasets, event_gps, event_detectors
from zenodo_get import download as zenodo_download
from gwosc.locate import get_event_urls
import asyncio
import concurrent.futures
from functools import partial
import gwpy.timeseries
from lalframe.utils import frtools
import os

import typer
from rich.console import Console

console = Console()

current_dir = os.getcwd()

pe_zenodo_releases = {}
pe_zenodo_releases['GWTC-2.1-confident'] = 'https://zenodo.org/records/6513631'
pe_zenodo_releases['GWTC-3-confident'] = 'https://zenodo.org/records/5546663'
pe_zenodo_releases['GWTC-4.0'] = 'https://zenodo.org/records/17014085'


async def _get_lvk_pe_data_async(event_name):
    output_dir = f"{current_dir}/{event_name}"
    if any(fname.endswith('.hdf5') for fname in os.listdir(output_dir)):
        typer.echo(f"PE data for '{event_name}' already exists in {output_dir}. Skipping download.")
        return

    catalogs = list(pe_zenodo_releases.keys()) + ['O4_Discovery_Papers']
    catalog = None

    for cat in catalogs:
        available_events = find_datasets(type='event', catalog=cat)
        if any(event_name in event for event in available_events):
            catalog = cat
            break

    if catalog is None:
        typer.echo(f"Event '{event_name}' not found in available catalogs.")
        raise typer.Exit(code=1)

    glob_pattern = f"*{event_name}*" if catalog == "GWTC-4.0" else f"*{event_name}*nocosmo*.h5"

    typer.echo(f"Queuing Zenodo download for {event_name}...")

    loop = asyncio.get_running_loop()

    download_func = partial(
        zenodo_download,
        record_id,
        output_dir=".",
        file_glob=glob_pattern
    )

    await loop.run_in_executor(None, download_func)

    typer.echo(f"Finished downloading PE data for {event_name}")


def _get_lvk_pe_data(event_name):
    output_dir = f"{current_dir}/{event_name}"
    if any(fname.endswith('.hdf5') for fname in os.listdir(output_dir)):
        typer.echo(f"PE data for '{event_name}' already exists in {output_dir}. Skipping download.")
        return

    catalogs = list(pe_zenodo_releases.keys()) + ['O4_Discovery_Papers']
    catalog = None

    for cat in catalogs:
        available_events = find_datasets(type='event', catalog=cat)
        if any(event_name in event for event in available_events):
            catalog = cat
            break

    if catalog is None:
        typer.echo(f"Event '{event_name}' not found in available catalogs.")
        raise typer.Exit(code=1)

    if catalog in pe_zenodo_releases:
        zenodo_url = pe_zenodo_releases[catalog]
        record_id = zenodo_url.split('/')[-1]

        typer.echo(f"Downloading PE data for '{event_name}' from Zenodo record {record_id}...")

        if catalog == "GWTC-4.0":
            zenodo_download(
                record_id,
                output_dir=f"{output_dir}",
                file_glob=f"*{event_name}*"
            )
        else:
            zenodo_download(
                record_id,
                output_dir=f"{output_dir}",
                file_glob=f"*{event_name}*nocosmo*.h5"
            )
        typer.echo(f"Download complete. Files saved to: {output_dir}")
    else:
        typer.echo(f"No Zenodo release mapping found for catalog '{catalog}'.")


def remove_duplicates(seq):
    # Each event has multiple versions, so the last bit in the name of '-vX'. We remove it and then remove duplicates.
    seen = set()
    result = []
    for item in seq:
        item_base = item.rsplit('-v', 1)[0]
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


def _get_lvk_info_individual(event_name):
    ''' Download info data for a specific event and save in the same folder as the strain data. '''
    if not os.path.exists(event_name):
        os.makedirs(event_name)
    filename = f"{event_name}/{event_name}_info.json"
    if os.path.exists(filename):
        typer.echo(f"File {filename} already exists.")
        with open(filename, 'r') as f:
            import json
            info = json.load(f)
            info['detectors'] = set(info['detectors'])
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


async def _get_lvk_strain_individual(event_name, return_data=False, download_pe=False, segment_length=60*20):
    ''' Download strain data for a specific event. '''
    info = _get_lvk_info_individual(event_name)
    events = info['event_name']
    gps = info['gps']
    detectors = info['detectors']
    typer.echo(f"Event '{event_name}' found with GPS time {gps}.")
    start, end = int(gps - segment_length/2), int(gps + segment_length/2)
    data = {}
    if not os.path.exists(event_name):
        os.makedirs(event_name)
    typer.echo(f"Downloading strain data for detectors: {', '.join(detectors)}")
    for det in detectors:
        if segment_length == 60*20:
            filename = f"{event_name}/{event_name}_{det}_strain.gwf"
        else:
            filename = f"{event_name}/{event_name}_{det}_strain_{segment_length//60}min.gwf"
        if os.path.exists(filename):
            typer.echo(f"File {filename} already exists.")
            if return_data == False:
                continue
            else:
                channel_name = frtools.get_channels(filename)[0]
                data[det] = gwpy.timeseries.TimeSeries.read(filename, format='gwf', channel=channel_name)
                typer.echo(f"Data loaded from {filename}.")
                continue
        typer.echo(f"Fetching {det} data from {start} to {end}...")
        try:
            data[det] = gwpy.timeseries.TimeSeries.fetch_open_data(det, start, end, cache=True)
            data[det].channel = f"{det}:GWOSC-STRAIN"
            data[det].write(filename, format='gwf')
            channel_name = data[det].channel
            typer.echo(f"Saved strain data to {filename} with channel {channel_name}.")
        except Exception as e:
            typer.echo(f"Error fetching data for {event_name} with {det}: {e}")
    if download_pe == True:
        _get_lvk_pe_data(event_name)
    if return_data:
        return data
    else:
        return None


def get_lvk_strain_individual_sync(event, download_pe=False, segment_length=60*20):
    return asyncio.run(_get_lvk_strain_individual(event, download_pe=download_pe, segment_length=segment_length))


async def _get_lvk_strain_all(download_pe=False, segment_length=60*20):
    events = _list_lvk_data()
    loop = asyncio.get_event_loop()
    with concurrent.futures.ProcessPoolExecutor(max_workers=32) as executor:  # FIXME: The ProcessPoolExecutor is used instead of ThreadPoolExecutor because the latter runs into issues with the zenodo's download function, which is not async safe (e.g., uses global variables). A separate process for each download seems to work because it isolates the memory. However, it's not ideal.
        tasks = [loop.run_in_executor(executor, get_lvk_strain_individual_sync, event, download_pe, segment_length) for event in events]
        results = await asyncio.gather(*tasks)
    data_all = dict(zip(events, results))
    return events, data_all


def get_lvk_strain(event_name, download_pe, segment_length=60*20):
    if event_name == 'all':
        events, data_all = asyncio.run(_get_lvk_strain_all(download_pe=download_pe, segment_length=segment_length))
        return events, data_all
    else:
        data = asyncio.run(_get_lvk_strain_individual(event_name, download_pe=download_pe, segment_length=segment_length))
        return event_name, data


def get_pe(event_name):
    events = check_event_name(event_name)
    datasets = find_datasets(type='pe', event_name=event_name)


def h5_to_dict(h5_obj):
    result = {}
    for key, item in h5_obj.items():
        if isinstance(item, h5py.Dataset):
            result[key] = item[()]
        elif isinstance(item, h5py.Group):
            result[key] = h5_to_dict(item)
    return result


def _load_pe_samples(event_name):
    output_dir = f"{current_dir}/{event_name}"
    pe_files = [fname for fname in os.listdir(output_dir) if fname.endswith('.hdf5') or fname.endswith('.h5')]
    if len(pe_files) == 0:
        typer.echo(f"No PE files found for event '{event_name}' in {output_dir}.")
        return None
    elif len(pe_files) > 1:
        typer.echo(f"Multiple PE files found for event '{event_name}' in {output_dir}. Using the first one: {pe_files[0]}")
    pe_file = pe_files[0]
    pe_path = os.path.join(output_dir, pe_file)
    print("pe_path:", pe_path)
    with h5py.File(pe_path, 'r') as f:
        approximants = ['NRSur7dq4', 'SEOBNRv4PHM', 'IMRPhenomXPHM', 'IMRPhenomPv2', 'IMRPhenomD']
        approximants = [f"C00:{approx}" for approx in approximants] + approximants
        dataset = None
        for approx in approximants:
            if approx in f:
                dataset = f[approx]
                break
        if dataset is None:
            typer.echo(f"WARNING: No known approximant found in PE file for event '{event_name}'. Available approximants: {list(f.keys())}")
            return None
        pe_samples = h5_to_dict(dataset)
        typer.echo(f"Loaded PE samples from {pe_path} using approximant {approx}.")
    return pe_samples


def _process_timeseries(event_name):
    info = _get_lvk_info_individual(event_name)
    detectors = info['detectors']
    data = asyncio.run(_get_lvk_strain_individual(event_name, return_data=True, download_pe=False))
    from . import plots
    fig, ax = plots.plot_strain(data)
    fig.savefig(f"{event_name}/{event_name}_strain.pdf", bbox_inches='tight')
    return None


def _process_psd_official(event_name):
    info = _get_lvk_info_individual(event_name)
    detectors = info['detectors']
    pe_samples = _load_pe_samples(event_name)
    if pe_samples is None:
        typer.echo(f"Cannot process PSD for event '{event_name}' because PE samples could not be loaded.")
        return None
    psds = pe_samples['psds']
    for det in detectors:
        if det in psds:
            f, psd = np.transpose(psds[det])
            np.save(f"{event_name}/{event_name}_{det}_psd.npy", psd)
            typer.echo(f"Saved PSD for {det} to {event_name}/{event_name}_{det}_psd.npy")
        else:
            typer.echo(f"WARNING: No PSD found for detector '{det}' in PE samples for event '{event_name}'. Available detectors in PE samples: {list(psds.keys())}")
    from . import plots
    fig, ax = plots.plot_psd(psds)
    fig.savefig(f"{event_name}/{event_name}_psd.pdf", bbox_inches='tight')
    return psds


def process_lvk_event(event_name):
    _process_timeseries(event_name)
    _process_psd_official(event_name)
    return None
