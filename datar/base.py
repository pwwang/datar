
from .core.load_plugins import plugin as _plugin
from .apis.base import *

locals().update(_plugin.hooks.base_api())
__all__ = [key for key in locals() if not key.startswith("_")]
_conflict_names = {"min", "max", "sum", "abs", "round", "all", "any", "re"}

if get_option("allow_conflict_names"):  # noqa: F405
    __all__.extend(_conflict_names)
    for name in _conflict_names:
        locals()[name] = locals()[name + "_"]


def __getattr__(name):
    """Even when allow_conflict_names is False, datar.base.sum should be fine
    """
    if name in _conflict_names:
        import sys
        import ast
        from executing import Source
        node = Source.executing(sys._getframe(1)).node
        if isinstance(node, (ast.Call, ast.Attribute)):
            # import datar.base as d
            # d.sum(...)
            return globals()[name + "_"]

    raise AttributeError
