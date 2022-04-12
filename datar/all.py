"""Import all constants, verbs and functions"""

_locs = locals()

from . import base as _base
_base_conflict_names = _base._conflict_names
for _key in _base.__all__:
    if _key not in _base_conflict_names:
        _locs[_key] = getattr(_base, _key)

from . import dplyr as _dplyr
_dplyr_conflict_names = _dplyr._conflict_names
for _key in _dplyr.__all__:
    if _key not in _dplyr_conflict_names:
        _locs[_key] = getattr(_dplyr, _key)

from .core.defaults import f
from .forcats import *
from .datar import *
from .tibble import *
from .tidyr import *
from .base import rank  # overwrite dplyr.rank

from .core.import_names_conflict import (
    handle_import_names_conflict as _handle_import_names_conflict,
)

__all__, _getattr = _handle_import_names_conflict(
    _locs,
    _base_conflict_names | _dplyr_conflict_names,
)

if _getattr is not None:
    __getattr__ = _getattr
