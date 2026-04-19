"""Top-level Python API for :mod:`gra`.

The package keeps the CLI entrypoint in ``gra.cli`` while also exposing the
library functions directly from ``import gra`` for interactive use.
"""

from importlib import import_module

_MODULE_EXPORTS = {"cli", "data", "data_lvk", "plots"}
_ATTR_EXPORTS = {
    "app": (".cli", "app"),
    "main": (".cli", "main"),
    "get_2mass_data": (".data", "get_2mass_data"),
    "get_lvk_strain": (".data", "get_lvk_strain"),
    "list_data_lvk": (".data", "list_data_lvk"),
    "process_lvk_event": (".data", "process_lvk_event"),
    "get_pe": (".data_lvk", "get_pe"),
    "h5_to_dict": (".data_lvk", "h5_to_dict"),
    "remove_duplicates": (".data_lvk", "remove_duplicates"),
    "plot_psd": (".plots", "plot_psd"),
    "plot_strain": (".plots", "plot_strain"),
}

__all__ = [
    "app",
    "cli",
    "data",
    "data_lvk",
    "get_2mass_data",
    "get_lvk_strain",
    "get_pe",
    "h5_to_dict",
    "hello",
    "list_data_lvk",
    "main",
    "plot_psd",
    "plot_strain",
    "plots",
    "process_lvk_event",
    "remove_duplicates",
]


def __getattr__(name):
    if name in _MODULE_EXPORTS:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module

    if name in _ATTR_EXPORTS:
        module_name, attr_name = _ATTR_EXPORTS[name]
        module = import_module(module_name, __name__)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | _MODULE_EXPORTS | set(_ATTR_EXPORTS))


def hello() -> str:
    return "Hello from gra!"
