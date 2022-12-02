from .core.plugin import plugin as _plugin
from .apis.dplyr import *

locals().update(_plugin.hooks.dplyr_api())

from .core.import_names_conflict import (
    handle_import_names_conflict as _handle_import_names_conflict
)

_conflict_names = {"filter", "slice"}

__all__, _getattr = _handle_import_names_conflict(locals(), _conflict_names)

if _getattr is not None:
    __getattr__ = _getattr
