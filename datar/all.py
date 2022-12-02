"""Import all constants, verbs and functions"""

_locs = locals()

from .core.defaults import f

from .base import *
from .dplyr import *
from .forcats import *
from .tibble import *
from .tidyr import *

from .core.import_names_conflict import (
    handle_import_names_conflict as _handle_import_names_conflict,
)
from .base import _conflict_names as _base_conflict_names
from .dplyr import _conflict_names as _dplyr_conflict_names

__all__, _getattr = _handle_import_names_conflict(
    _locs,
    _base_conflict_names | _dplyr_conflict_names,
)

if _getattr is not None:
    __getattr__ = _getattr
