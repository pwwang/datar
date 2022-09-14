"""Public APIs from this submodule"""
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
    prod,
    sign,
    signif,
    trunc,
    exp,
    log,
    log2,
    log10,
    log1p,
    std,
    sd,
    weighted_mean,
    quantile,
)
from .bessel import bessel_i, bessel_j, bessel_k, bessel_y
from .casting import as_double, as_float, as_int, as_integer, as_numeric
from .complex import arg, as_complex, conj, im, is_complex, mod, re as re_
from .constants import LETTERS, letters, month_abb, month_name, pi
from .cum import cummax, cummin, cumprod, cumsum
from .date import as_date, as_pd_date
from .factor import (
    as_categorical,
    as_factor,
    droplevels,
    factor,
    is_categorical,
    is_factor,
    is_ordered,
    levels,
    nlevels,
    ordered,
)
from .funs import (
    cut,
    diff,
    expandgrid,
    identity,
    make_unique,
    make_names,
    rank,
    outer,
)
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
from .na import NA, NaN, any_na, is_na, Inf, is_finite, is_infinite, is_nan
from .null import NULL, as_null, is_null
from .random import set_seed
from .rep import rep
from .seq import (
    c,
    length,
    lengths,
    order,
    rev,
    sample,
    seq,
    seq_along,
    seq_len,
    sort,
    # unique,
    match,
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
from .stats import rnorm, rpois, runif
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
    startswith,
    endswith,
    strtoi,
    trimws,
    chartr,
    tolower,
    toupper,
)
from .table import table, tabulate
from .testing import (
    is_atomic,
    is_scalar,
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
    append,
    colnames,
    diag,
    dim,
    duplicated,
    intersect,
    names,
    ncol,
    nrow,
    prop_table,
    proportions,
    rownames,
    setdiff,
    setequal,
    unique,
    t,
    union,
    max_col,
    complete_cases,
    head,
    tail,
)
from .which import which, which_max, which_min

from ..core.import_names_conflict import (
    handle_import_names_conflict as _handle_import_names_conflict
)

_conflict_names = {"min", "max", "sum", "abs", "round", "all", "any", "re"}

__all__, _getattr = _handle_import_names_conflict(locals(), _conflict_names)

if _getattr is not None:
    __getattr__ = _getattr
