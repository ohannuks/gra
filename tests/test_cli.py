"""
CLI smoke tests using typer's CliRunner.

All data-layer functions are mocked so these tests exercise CLI wiring
(argument parsing, option handling, exit codes) without any I/O.
"""

from unittest.mock import patch

from typer.testing import CliRunner

from gra.cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# data ls lvk
# ---------------------------------------------------------------------------

def test_data_ls_lvk():
    with patch("gra.data.list_data_lvk", return_value=["GW150914", "GW151226"]) as mock:
        result = runner.invoke(app, ["data", "ls", "lvk"])
    assert result.exit_code == 0
    mock.assert_called_once()


# ---------------------------------------------------------------------------
# data get lvk
# ---------------------------------------------------------------------------

def test_data_get_lvk_default_options():
    with patch("gra.data.get_lvk_strain", return_value=("GW150914", {})) as mock:
        result = runner.invoke(app, ["data", "get", "lvk", "GW150914"])
    assert result.exit_code == 0
    mock.assert_called_once_with("GW150914", True, 1200)


def test_data_get_lvk_no_pe_flag():
    with patch("gra.data.get_lvk_strain", return_value=("GW150914", {})) as mock:
        result = runner.invoke(app, ["data", "get", "lvk", "GW150914", "--no-pe"])
    assert result.exit_code == 0
    mock.assert_called_once_with("GW150914", False, 1200)


def test_data_get_lvk_custom_segment_length():
    with patch("gra.data.get_lvk_strain", return_value=("GW150914", {})) as mock:
        result = runner.invoke(app, ["data", "get", "lvk", "GW150914", "--segment-length", "600"])
    assert result.exit_code == 0
    mock.assert_called_once_with("GW150914", True, 600)


def test_data_get_lvk_all_events():
    with patch("gra.data.get_lvk_strain", return_value=(["GW150914"], {})) as mock:
        result = runner.invoke(app, ["data", "get", "lvk", "all"])
    assert result.exit_code == 0
    mock.assert_called_once_with("all", True, 1200)


# ---------------------------------------------------------------------------
# data process lvk
# ---------------------------------------------------------------------------

def test_data_process_lvk():
    with patch("gra.data.process_lvk_event", return_value=None) as mock:
        result = runner.invoke(app, ["data", "process", "lvk", "GW150914"])
    assert result.exit_code == 0
    mock.assert_called_once_with("GW150914")


# ---------------------------------------------------------------------------
# data get 2mass
# ---------------------------------------------------------------------------

def test_data_get_2mass():
    with patch("gra.data.get_2mass_data", return_value=None) as mock:
        result = runner.invoke(app, ["data", "get", "2mass", "spectroscopic"])
    assert result.exit_code == 0
    mock.assert_called_once_with("spectroscopic")
