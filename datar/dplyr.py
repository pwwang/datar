
from .core.load_plugins import plugin as _plugin
from .core.options import get_option as _get_option
from .apis.dplyr import *

locals().update(_plugin.hooks.dplyr_api())
__all__ = [key for key in locals() if not key.startswith("_")]
_conflict_names = {"filter", "slice"}

if _get_option("allow_conflict_names"):  # pragma: no cover
    __all__.extend(_conflict_names)
    for name in _conflict_names:
        locals()[name] = locals()[name + "_"]
