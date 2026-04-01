"""
Unit tests for gra.data (2MASS helpers).

File-system and network calls are mocked via tmp_path / monkeypatch so no
real downloads or data files are required.
"""

import os
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# get_2mass_data routing
# ---------------------------------------------------------------------------

def test_get_2mass_data_routes_spectroscopic():
    from gra.data import get_2mass_data
    with patch("gra.data._get_2mass_spectroscopic", return_value=None) as mock:
        get_2mass_data("spectroscopic")
    mock.assert_called_once()


def test_get_2mass_data_individual_raises():
    from gra.data import get_2mass_data
    with pytest.raises(ValueError, match="Not implemented"):
        get_2mass_data("GW150914")


# ---------------------------------------------------------------------------
# _get_2mass_spectroscopic  (file already cached)
# ---------------------------------------------------------------------------

def test_get_2mass_spectroscopic_cached_no_return(tmp_path, monkeypatch):
    """When the CSV exists and return_data=False, return None without loading."""
    from gra import data as data_mod

    catalog_dir = tmp_path / "2mass"
    catalog_dir.mkdir()
    csv = catalog_dir / "2mass_galaxy_catalog_spec.csv"
    csv.write_text("col1,col2\n1,2\n")

    monkeypatch.chdir(tmp_path)

    result = data_mod._get_2mass_spectroscopic(return_data=False)
    assert result is None


def test_get_2mass_spectroscopic_cached_with_return(tmp_path, monkeypatch):
    """When the CSV exists and return_data=True, return the loaded table."""
    from astropy.table import Table
    from gra import data as data_mod

    catalog_dir = tmp_path / "2mass"
    catalog_dir.mkdir()
    csv = catalog_dir / "2mass_galaxy_catalog_spec.csv"
    # Write a minimal valid CSV that astropy can read
    csv.write_text("col1 col2\n1 2\n3 4\n")

    monkeypatch.chdir(tmp_path)

    mock_table = MagicMock(spec=Table)
    with patch("astropy.table.Table.read", return_value=mock_table):
        result = data_mod._get_2mass_spectroscopic(return_data=True)

    assert result is mock_table
