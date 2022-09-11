"""Windowed rank functions.

See https://github.com/tidyverse/dplyr/blob/master/R/rank.R
"""
from pipda import register_func, Symbolic

from ..core.contexts import Context

from ._rank import (
    _row_number,
    _ntile,
    _rank,
    _percent_rank,
    _cume_dist,
)


@register_func(context=Context.EVAL)
def row_number(x=Symbolic()):
    """Gives the row number

    See https://dplyr.tidyverse.org/reference/ranking.html

    Args:
        x: a vector of values to rank.

    Returns:
        The row number of `x` or the data frame (0-based)
    """
    return _row_number(x)


@register_func(context=Context.EVAL)
def ntile(x=Symbolic(), *, n=None):
    """A rough rank, which breaks the input vector into `n` buckets.

    Note:
        The output tiles are 1-based.
        The result is slightly different from dplyr's ntile.
        >>> ntile(c(1,2,NA,1,0,NA), 2) # dplyr
        >>> # 1 2 NA 2 1 NA
        >>> ntile([1,2,NA,1,0,NA], n=2) # datar
        >>> # [1, 2, NA, 1, 1, NA]
        >>> # Categories (2, int64): [1 < 2]
    """
    return _ntile(x, n)


@register_func(context=Context.EVAL)
def min_rank(x=Symbolic(), *, na_last="keep"):
    """Rank the data using min method"""
    return _rank(x, na_last=na_last, method="min")


@register_func(context=Context.EVAL)
def dense_rank(x=Symbolic(), *, na_last="keep"):
    """Rank the data using dense method"""
    return _rank(x, na_last=na_last, method="dense")


@register_func(context=Context.EVAL)
def percent_rank(x=Symbolic(), *, na_last="keep"):
    """Rank the data using percent_rank method"""
    return _percent_rank(x, na_last)


@register_func(context=Context.EVAL)
def cume_dist(x=Symbolic(), *, na_last="keep"):
    """Rank the data using percent_rank method"""
    return _cume_dist(x, na_last)
