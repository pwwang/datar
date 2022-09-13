"""Provide options"""

from typing import Any, Generator, Mapping, Union, Callable
from contextlib import contextmanager
from pathlib import Path

from diot import Diot
from simpleconf import Config

_key_transform = lambda key: key.replace("_", ".")
_dict_transform_back = lambda dic: {
    key.replace(".", "_"): val for key, val in dic.items()
}


def enable_pdtypes_callback(enable: bool) -> None:  # pragma: no cover
    from .utils import logger

    try:
        import pdtypes
    except ImportError:
        pdtypes = None

    if enable and pdtypes is None:
        logger.warning(
            "Package `pdtypes` not installed for `options(enable_pdtypes=True)`"
        )
    elif not enable and pdtypes is not None:
        pdtypes.unpatch()


OPTION_CALLBACKS = Diot(
    # allow 'a.b' to access 'a_b'
    diot_transform=_key_transform,
    enable_pdtypes=enable_pdtypes_callback,
)


OPTION_FILE_HOME = Path("~/.datar.toml").expanduser()
OPTION_FILE_CWD = Path("./.datar.toml").resolve()
OPTIONS = Diot(
    # Whether use 0-based numbers when index is involved, acts similar like R
    dplyr_summarise_inform=True,
    # What to do when there are conflicts importing names
    # - `warn`: show warnings
    # - `silent`: ignore the conflicts
    # - `underscore_suffixed`: add suffix `_` to the conflicting names
    #   (and don't do any warnings)
    import_names_conflict="warn",
    # Enable pdtypes
    enable_pdtypes=True,
    # add_option=True,
    # allow 'a.b' to access 'a_b'
    # diot_transform=_key_transform,
    # The backend for datar
    backend="pandas",  # or "modin"
)
OPTIONS = Diot(
    Config.load(
        OPTIONS,
        OPTION_FILE_HOME,
        OPTION_FILE_CWD,
        ignore_nonexist=True,
    ),
    diot_transform=_key_transform,
)


def apply_init_callbacks():
    """Apply the callbacks when options are initialized"""
    for key in OPTION_CALLBACKS:
        OPTION_CALLBACKS[key](OPTIONS[key])


def options(
    *args: Union[str, Mapping[str, Any]],
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
        callback = OPTION_CALLBACKS.get(key)
        if callable(callback):  # pragma: no cover, already applied
            callback(val)

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
    """Get the current value set for option ‘x’,
    or ‘default’ (which defaults to ‘NULL’) if the option is unset.

    Args:
        x: The name of the option
        default: The default value if `x` is unset
    """
    return OPTIONS.get(x, default)


def add_option(x: str, default: Any = None, callback: Callable = None) -> None:
    """Add an option"""
    OPTIONS[x] = default
    if callback:
        OPTION_CALLBACKS[x] = callback
        callback(default)
