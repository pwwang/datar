"""Import all constants, verbs and functions"""

_locs = locals()

from .core import load_plugins as _
from .core.defaults import f
from .core.import_names_conflict import (
    handle_import_names_conflict as _handle_import_names_conflict,
)

from . import base as _base, dplyr as _dplyr

from .forcats import *
from .tibble import *
from .tidyr import *
from .misc import *

_locs.update(
    {
        key: getattr(_base, key)
        for key in _base.__all__
        if key not in _base._conflict_names
    }
)
_locs.update(
    {
        key: getattr(_dplyr, key)
        for key in _dplyr.__all__
        if key not in _dplyr._conflict_names
    }
)
__all__, _getattr = _handle_import_names_conflict(
    _locs,
    _base._conflict_names | _dplyr._conflict_names,
)

if _getattr is not None:
    __getattr__ = _getattr
