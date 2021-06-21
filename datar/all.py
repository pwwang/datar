"""Import all constants, verbs and functions"""
# pylint: disable=wildcard-import, unused-wildcard-import
# pylint: disable=unused-import, reimported, invalid-name
from . import f
from .base import _no_warn as _
from .base import *
from .base import _warn as _
from .utils import *
from .stats import *
from .dplyr import _no_warn as _
from .dplyr import *
from .dplyr import _warn as _
from .tidyr import *
from .tibble import *
from .datar import *

from . import base as _base, dplyr as _dplyr
from .core.warn_builtin_names import warn_builtin_names as _warn_builtin_names

_builtin_names = dict(
    filter=_dplyr.dfilter,
    slice=_dplyr.dslice,
    min=_base.arithmetic,
    max=_base.arithmetic,
    sum=_base.arithmetic,
    abs=_base.arithmetic,
    round=_base.arithmetic,
    all=_base.testing,
    any=_base.testing,
    re=_base.complex
)
__all__ = [name for name in locals() if not name.startswith('_')]

for name in _builtin_names:
    # let __getattr__ handles the builtins
    del locals()[name]

__getattr__ = _warn_builtin_names(**_builtin_names)
