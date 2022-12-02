"""Utilities for datar"""
import sys
import logging
from typing import Any, Callable

from .plugin import plugin

# logger
logger = logging.getLogger("datar")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s][%(name)s][%(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(stream_handler)


class NotImplementedByCurrentBackendError(NotImplementedError):
    """Raised when a function is not implemented by the current backend"""

    def __init__(self, func: str, data: Any = None) -> None:
        data_msg = ""
        if data is not None:
            data_msg = f"data type: {type(data).__name__}, "
        msg = (
            f"'{func}' "
            f"({data_msg}backends: "
            f"{', '.join(plugin.get_enabled_plugin_names())})"
        )
        super().__init__(msg)


class CollectionFunction:
    """Enables c[1:3] to be interpreted as 1:3"""

    def __init__(self, c_func: Callable) -> None:
        self.c = c_func

    def __call__(self, *args, **kwargs):
        kwargs["__ast_fallback"] = "normal"
        return self.c(*args, **kwargs)

    def __getitem__(self, item):
        """Allow c[1:3] to be interpreted as 1:3"""
        return plugin.hooks.c_getitem(item)


def arg_match(arg, argname, values, errmsg=None):
    """Make sure arg is in one of the values.

    Mimics `rlang::arg_match`.
    """
    if not errmsg:
        values = list(values)
        errmsg = f"`{argname}` must be one of {values}."
    if arg not in values:
        raise ValueError(errmsg)
    return arg
