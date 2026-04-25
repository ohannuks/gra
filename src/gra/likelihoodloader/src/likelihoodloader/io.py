"""Posterior H5 and GWF I/O helpers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

import bilby.core.utils.io
import h5py
from boltons.iterutils import research
from gwpy.timeseries import TimeSeries
from lalframe.utils import frtools

PRIORITY_WAVEFORMS = [
    "NRSur7dq4",
    "SEOBNRv5PHM",
    "SEOBNRv4PHM",
    "IMRPhenomXPHM",
]


def h5_to_dict(h5_obj: h5py.Group | h5py.File) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in h5_obj.items():
        if isinstance(item, h5py.Dataset):
            result[key] = item[()]
        elif isinstance(item, h5py.Group):
            result[key] = h5_to_dict(item)
    return result


def _choose_priority_waveform(data_dict: dict[str, Any]) -> dict[str, Any]:
    for waveform in PRIORITY_WAVEFORMS:
        if waveform in data_dict or "C00:" + waveform in data_dict:
            if waveform in data_dict:
                return data_dict[waveform]
            return data_dict["C00:" + waveform]
    raise KeyError(
        f"No known waveform in PE file. Tried: {PRIORITY_WAVEFORMS}. "
        f"Top-level keys: {list(data_dict.keys())}"
    )


def load_posterior_dict(posterior_path: str | os.PathLike) -> dict[str, Any]:
    with h5py.File(posterior_path, "r") as h5file:
        root = bilby.core.utils.io.recursively_load_dict_contents_from_group(
            h5file, "/"
        )
    return _choose_priority_waveform(root)


def read_gwf_file(path: str | os.PathLike) -> TimeSeries:
    path = os.fspath(path)
    channel = frtools.get_channels(path)[0]
    return TimeSeries.read(path, format="gwf", channel=channel)


def find_dictionary(data_dict: dict[str, Any], key: str) -> list:
    return research(data_dict, query=lambda p, k, v: k == key)  # type: ignore[no-any-return]


def find_dictionary_partial(data_dict: dict[str, Any], key: str) -> list:
    return research(  # type: ignore[no-any-return]
        data_dict, query=lambda p, k, v: key in k
    )
