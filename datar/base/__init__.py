"""Public APIs from this submodule"""
# pylint: disable=unused-import
from ..core.options import get_option, options, options_context
from .arithmetic import (
    ceiling,
    cov,
    floor,
    mean,
    median,
    pmax,
    pmin,
    sqrt,
    var,
    scale,
    col_sums,
    row_sums,
    col_means,
    row_means,
    col_sds,
    row_sds,
    col_medians,
    row_medians,
    min as min_,
    max as max_,
    round as round_,
    sum as sum_,
    abs as abs_,
)
from .bessel import bessel_i, bessel_j, bessel_k, bessel_y
from .casting import as_double, as_float, as_int, as_integer, as_numeric
from .complex import arg, as_complex, conj, im, is_complex, mod, re as re_
from .constants import LETTERS, Inf, letters, month_abb, month_name, pi
from .cum import cummax, cummin, cumprod, cumsum
from .date import as_date
from .factor import (
    as_categorical,
    as_factor,
    droplevels,
    factor,
    is_categorical,
    is_factor,
    levels,
)
from .funs import cut, data_context, expandgrid, identity
from .logical import (
    FALSE,
    TRUE,
    as_bool,
    as_logical,
    is_bool,
    is_false,
    is_logical,
    is_true,
)
from .na import NA, NaN, any_na, is_na
from .null import NULL, as_null, is_null
from .random import set_seed
from .seq import (
    c,
    length,
    lengths,
    rep,
    rev,
    sample,
    seq,
    seq_along,
    seq_len,
    unique,
)
from .special import (
    beta,
    choose,
    digamma,
    factorial,
    gamma,
    lbeta,
    lchoose,
    lfactorial,
    lgamma,
    psigamma,
    trigamma,
)
from .string import (
    as_character,
    as_str,
    as_string,
    grep,
    grepl,
    gsub,
    is_character,
    is_str,
    is_string,
    nchar,
    nzchar,
    paste,
    paste0,
    sprintf,
    strsplit,
    sub,
    substr,
    substring,
)
from .table import table
from .testing import (
    is_atomic,
    is_double,
    is_element,
    is_float,
    is_in,
    is_int,
    is_integer,
    is_numeric,
    any as any_,
    all as all_,
)
from .trig_hb import (
    acos,
    acosh,
    asin,
    asinh,
    atan,
    atan2,
    atanh,
    cos,
    cosh,
    cospi,
    sin,
    sinh,
    sinpi,
    tan,
    tanh,
    tanpi,
)
from .verbs import (
    colnames,
    diag,
    dim,
    duplicated,
    intersect,
    names,
    ncol,
    nrow,
    rownames,
    setdiff,
    setequal,
    t,
    union,
)
from .which import which, which_max, which_min

__all__ = [name for name in locals() if not name.startswith("_")]

_builtin_names = {  # pylint: disable=invalid-name
    "min": min_,
    "max": max_,
    "sum": sum_,
    "abs": abs_,
    "round": round_,
    "all": all_,
    "any": any_,
    "re": re_,
}
__all__.extend(_builtin_names)

# warn when builtin names are imported directly
# pylint: disable=wrong-import-position
from ..core.warn_builtin_names import warn_builtin_names

__getattr__ = warn_builtin_names(**_builtin_names)
