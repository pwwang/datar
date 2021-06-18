"""Public APIs from this submodule"""
# pylint: disable=unused-import,redefined-builtin
from .constants import (
    TRUE, FALSE, pi, Inf, letters, LETTERS,
)
from ..core.options import options, getOption, options_context
from .verbs import (
    colnames, rownames, dim, nrow, ncol, diag, t, names,
    intersect, union, setdiff, setequal, duplicated, cov
)
from .funs import (
    as_date, as_character, as_double, as_factor, as_categorical,
    as_int, as_integer, as_logical, as_bool, as_numeric,
    c, ceiling, floor, cummax, cummin, cumprod, cumsum, cut, sample,
    is_categorical, is_character, is_double, is_factor, is_float,
    is_int, is_int64, is_integer, is_numeric,
    as_int64, unique, Im, Re, length, lengths,
    seq_along, seq_len, seq, abs, round, sqrt, rev,
    droplevels, levels,
    sin, cos, identity, expandgrid
)
# plain functions
from .funs import factor, rep, context
from .table import table
from .arithmetic import sum, mean, median, min, max, pmin, pmax, var
from .which import which, which_min, which_max, is_element, is_in, all, any


from .bessel import bessel_i, bessel_j, bessel_k, bessel_y
from .funs import istrue, isfalse
from .na import NA, NaN, is_na, any_na
from .null import NULL, is_null, as_null
