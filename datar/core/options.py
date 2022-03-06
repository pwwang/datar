"""Provide options"""

from typing import Any, Generator, Mapping, Union, Callable
from contextlib import contextmanager

from diot import Diot
from pipda import options as pipda_options


_key_transform = lambda key: key.replace("_", ".")
_dict_transform_back = lambda dic: {
    key.replace(".", "_"): val for key, val in dic.items()
}

OPTIONS = Diot(
    # Whether use 0-based numbers when index is involved, acts similar like R
    dplyr_summarise_inform=True,
    # whether warn about importing functions that override builtin ones.
    warn_builtin_names=True,
    #
    enable_pdtypes=False,
    # add_option=True,
    # allow 'a.b' to access 'a_b'
    diot_transform=_key_transform,
    # Warn about failure to get ast node
    warn_astnode_failure=True,
    # All piping mode:
    # - Assume all verbs are using PIPING_VERB env
    # - Assume all data functions are using PIPING env
    # - Assume all non-data functions are using PIPING verbs
    # This is useful when source code is not available.
    assume_all_piping=False,
)


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
    warn_astnode_failure=lambda val: setattr(
        pipda_options, "warn_astnode_failure", val
    ),
    assume_all_piping=lambda val: setattr(
        pipda_options, "assume_all_piping", val
    ),
)


def options(
    *args: Union[str, Mapping[str, Any]],
    **kwargs: Any,
) -> Mapping[str, Any]:
    """Allow the user to set and examine a variety of global options

    Args:
        *args: Names of options to return
        **kwargs: name-value pair to create/set an option
    """
    if not args and not kwargs:
        return OPTIONS.copy()

    names = [arg.replace(".", "_") for arg in args if isinstance(arg, str)]
    pairs = {}
    for arg in args:
        if isinstance(arg, dict):
            pairs.update(_dict_transform_back(arg))
    pairs.update(_dict_transform_back(kwargs))

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
        if callable(callback):
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
