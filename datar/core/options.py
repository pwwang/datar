"""Provide options"""
from __future__ import annotations

from typing import Any, Generator, Mapping
from contextlib import contextmanager

from diot import Diot
from simpleconf import Config

from .defaults import OPTION_FILE_CWD, OPTION_FILE_HOME

_key_transform = lambda key: key.replace("_", ".")
_dict_transform_back = lambda dic: {
    key.replace(".", "_"): val for key, val in dic.items()
}

OPTIONS = Diot(
    Config.load(
        {
            # Do we allow to use conflict names directly?
            "allow_conflict_names": False,
            # Disable some installed backends
            "backends": [],
        },
        OPTION_FILE_HOME,
        OPTION_FILE_CWD,
        ignore_nonexist=True,
    ),
    diot_transform=_key_transform,
)


def options(
    *args: str | Mapping[str, Any],
    _return: bool = None,
    **kwargs: Any,
) -> Mapping[str, Any]:
    """Allow the user to set and examine a variety of global options

    Args:
        *args: Names of options to return
        **kwargs: name-value pair to create/set an option
        _return: Whether return the options.
            If `None`, turned to `True` when option names provided in `args`.

    Returns:
        The options before updating if `_return` is `True`.
    """
    if not args and not kwargs and (_return is None or _return is True):
        # Make sure the options won't be changed
        return OPTIONS.copy()

    names = [arg.replace(".", "_") for arg in args if isinstance(arg, str)]
    pairs = {}
    for arg in args:
        if isinstance(arg, dict):
            pairs.update(_dict_transform_back(arg))
    pairs.update(_dict_transform_back(kwargs))

    out = None
    if _return is None:
        _return = names

    if _return:
        out = Diot(
            {
                name: value
                for name, value in OPTIONS.items()
                if name in names or name in pairs
            },
            diot_transform=_key_transform,
        )

    for key, val in pairs.items():
        oldval = OPTIONS[key]
        if oldval == val:
            continue
        OPTIONS[key] = val

    return out


@contextmanager
def options_context(**kwargs: Any) -> Generator:
    """A context manager to execute code with temporary options

    Note that this is not thread-safe.
    """
    opts = options()  # type: Mapping[str, Any]
    options(**kwargs)
    yield
    options(opts)


def get_option(x: str, default: Any = None) -> Any:
    """Get the current value set for option `x`,
    or `default` (which defaults to `NULL`) if the option is unset.

    Args:
        x: The name of the option
        default: The default value if `x` is unset
    """
    return OPTIONS.get(x, default)


def add_option(x: str, default: Any = None) -> None:
    """Add an option

    Args:
        x: The name of the option
        default: The default value if `x` is unset
    """
    OPTIONS.setdefault(x, default)
