"""Public APIs from this submodule"""
# pylint: disable=unused-import,redefined-builtin
from .constants import (
    NA, TRUE, FALSE, NULL, pi, Inf, letters, LETTERS,
    NA_character_,
    NA_compex_,
    NA_integer_,
    NA_real_
)
from .options import options, getOption, options_context
from .verbs import (
    colnames, rownames, dim, nrow, ncol, diag, t, names,
    intersect, union, setdiff, setequal
)
from .funcs import (
    as_date, as_character, as_double, as_factor, as_categorical,
    as_int, as_integer, as_logical, as_bool, table, as_numeric,
    c, ceiling, floor, cummax, cummin, cumprod, cumsum, cut, sample,
    is_categorical, is_character, is_double, is_factor, is_float,
    is_int, is_int64, is_integer, is_na, is_numeric, sum, mean, median,
    min, max, as_int64, unique, Im, Re, is_in, is_element, length, lengths,
    seq_along, seq_len, seq, abs, pmax, pmin, round, sqrt, rev,
    droplevels, levels, sin, cos, identity, expandgrid, all, any
)
# plain functions
from .funcs import factor, rep, context
