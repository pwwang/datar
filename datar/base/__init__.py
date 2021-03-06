"""Public APIs from this submodule"""
# pylint: disable=unused-import
from .constants import NA, TRUE, FALSE, NULL, pi, Inf, letters, LETTERS
from .verbs import colnames, rownames, dim, nrow, ncol, diag, t
from .funcs import (
    as_date, as_character, as_double, as_factor, as_categorical,
    as_int, as_integer, as_logical, as_bool, table,
    c, ceiling, floor, cummax, cummin, cumprod, cumsum, cut, sample,
    is_categorical, is_character, is_double, is_factor, is_float,
    is_int, is_na, is_numeric, sum, mean, min, max, as_int64,
    seq_along, seq_len, seq, abs, pmax, pmin, round, sqrt,
    droplevels, sin, cos
)
# plain functions
from .funcs import factor, rep, context
