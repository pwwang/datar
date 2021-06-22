"""Public APIs from this submodule"""
# pylint: disable=unused-import
from .constants import (
    pi, Inf, letters, LETTERS, month_abb, month_name
)
from ..core.options import options, get_option, options_context
from .verbs import (
    colnames, rownames, dim, nrow, ncol, diag, t, names,
    intersect, union, setdiff, setequal, duplicated
)
from .funs import (
    cut, identity, expandgrid, data_context
)
from .arithmetic import mean, median, pmin, pmax, var, ceiling, floor, sqrt, cov
from .bessel import bessel_i, bessel_j, bessel_k, bessel_y
from .casting import as_integer, as_float, as_double, as_int, as_numeric
from .complex import im, mod, arg, conj, is_complex, as_complex
from .cum import cumsum, cumprod, cummin, cummax
from .date import as_date
from .factor import (
    factor, droplevels, levels,
    is_factor, is_categorical, as_factor, as_categorical
)
from .logical import (
    TRUE, FALSE,
    is_true, is_false,
    is_bool, is_logical,
    as_bool, as_logical
)
from .na import NA, NaN, is_na, any_na
from .null import NULL, is_null, as_null
from .random import set_seed
from .seq import (
    c, seq, seq_len, seq_along, rev, rep, lengths, unique, sample, length
)

from .special import (
    beta, lbeta, gamma, lgamma, digamma, trigamma, psigamma,
    choose, lchoose, factorial, lfactorial
)
from .string import (
    is_str, is_string, is_character, as_str, as_string, as_character,
    grep, grepl, sub, gsub, nchar, nzchar, paste, paste0, sprintf,
    substr, substring, strsplit
)
from .table import table
from .testing import (
    is_double, is_float, is_int, is_integer, is_numeric,
    is_atomic, is_element, is_in
)
from .trig_hb import (
    cos, sin, tan, acos, asin, atan, atan2, cospi, sinpi, tanpi,
    cosh, sinh, tanh, acosh, asinh, atanh
)
from .which import which, which_min, which_max

__all__ = [name for name in locals() if not name.startswith('_')]
__all__.extend([
    "min",
    "max",
    "sum",
    "abs",
    "round",
    "all",
    "any",
    "re",
])

# warn when builtin names are imported directly
# pylint: disable=wrong-import-position
from ..core.warn_builtin_names import warn_builtin_names
from . import (
    arithmetic as _arithmetic,
    testing as _testing,
    complex as _complex
)

__getattr__ = warn_builtin_names(
    min=_arithmetic,
    max=_arithmetic,
    sum=_arithmetic,
    abs=_arithmetic,
    round=_arithmetic,
    all=_testing,
    any=_testing,
    re=_complex
)
