"""Import all constants, verbs and functions"""
# pylint: disable=wildcard-import, unused-wildcard-import
# pylint: disable=unused-import, reimported, invalid-name
from . import f
from .base import _no_warn as _  # don't override from datar.all import _no_warn
from .base import _builtin_names as _base_builtin_names
from .base import *
from .base import _warn as _
from .datar import *
from .dplyr import _no_warn as _
from .dplyr import _builtin_names as _dplyr_builtin_names
from .dplyr import *
from .dplyr import _warn as _
from .stats import *
from .tibble import *
from .tidyr import *
from .utils import *

_builtin_names = _base_builtin_names.copy()
_builtin_names.update(_dplyr_builtin_names)
# builtin names included
__all__ = [var_ for var_ in locals() if not var_.startswith("_")]

for name in _builtin_names:
    # let __getattr__ handles the builtins, otherwise
    # from datar.all import filter
    # will not warn
    del locals()[name]

from .core.warn_builtin_names import (  # pylint: disable=wrong-import-position
    warn_builtin_names as _warn_builtin_names,
)

__getattr__ = _warn_builtin_names(**_builtin_names)
