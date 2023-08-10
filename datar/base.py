
from .core.load_plugins import plugin as _plugin
from .apis.base import *

locals().update(_plugin.hooks.base_api())
__all__ = [key for key in locals() if not key.startswith("_")]
_conflict_names = {"min", "max", "sum", "abs", "round", "all", "any", "re"}

if get_option("allow_conflict_names"):  # noqa: F405 pragma: no cover
    __all__.extend(_conflict_names)
    for name in _conflict_names:
        locals()[name] = locals()[name + "_"]
