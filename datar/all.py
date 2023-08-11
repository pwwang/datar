"""Import all constants, verbs and functions"""

from .core import load_plugins as _
from .core.defaults import f

from .base import _conflict_names as _base_conflict_names
from .dplyr import _conflict_names as _dplyr_conflict_names

from .base import *
from .dplyr import *
from .forcats import *
from .tibble import *
from .tidyr import *
from .misc import *

__all__ = [key for key in locals() if not key.startswith("_")]

if get_option("allow_conflict_names"):  # noqa: F405
    __all__.extend(_base_conflict_names | _dplyr_conflict_names)
    for name in _base_conflict_names | _dplyr_conflict_names:
        locals()[name] = locals()[name + "_"]


def __getattr__(name):
    """Even when allow_conflict_names is False, datar.base.sum should be fine
    """
    if name in _base_conflict_names | _dplyr_conflict_names:
        import sys
        import ast
        from executing import Source
        node = Source.executing(sys._getframe(1)).node
        if isinstance(node, (ast.Call, ast.Attribute)):
            # import datar.all as d
            # d.sum(...) or getattr(d, "sum")(...)
            return globals()[name + "_"]

    raise AttributeError
