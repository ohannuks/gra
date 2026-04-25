"""Run `uv run pe_lvk.py` from the repository root (forwards to ``gra.pe_lvk``)."""

import pathlib
import runpy

_root = pathlib.Path(__file__).resolve().parent
runpy.run_path(_root / "src" / "gra" / "pe_lvk.py", run_name="__main__")
