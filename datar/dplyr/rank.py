"""Windowed rank functions.

See https://github.com/tidyverse/dplyr/blob/master/R/slice.R
"""
from typing import Any, Iterable, Optional

import numpy
import pandas
from pandas.core.frame import DataFrame
from pipda import register_func
from pipda.utils import Expression

from ..core.contexts import Context
# from ..core.types import is_scalar
from ..base.funcs import NA

@register_func(context=Context.EVAL)
def row_number(
        _data: Iterable[Any],
        series: Optional[Iterable[Any]] = None
) -> numpy.ndarray:
    """Gives the row number, 1-based."""
    if series is None:
        return numpy.array(range(len(_data))) + 1
    return _rank(series, na_last="keep", method="first")

@register_func(context=Context.EVAL)
def ntile(
        _data: Iterable[Any],
        series: Optional[Iterable[Any]] = None,
        n: Optional[int] = None
) -> Iterable[Any]:
    """A rough rank, which breaks the input vector into ‘n’ buckets.

    Note:
        The output tiles are 0-based.
        The result is slightly different from dplyr's ntile.
        >>> ntile(c(1,2,NA,1,0,NA), 2) # dplyr
        >>> # 1 2 NA 2 1 NA
        >>> ntile([1,2,NA,1,0,NA], n=2) # datar
        >>> # [0, 1, NaN, 0, 0, NaN]
        >>> # Categories (2, int64): [0 < 1]
    """
    if isinstance(series, int) and n is None:
        n = series
        series = None

    if series is None:
        if isinstance(_data, Expression):
            raise TypeError(
                "`ntile` with a given iterable should be used in "
                "a piping evironment "
                "(i.e. df >> mutate(nt=ntile(row_number(), n=3)))."
            )
        series = row_number(_data) if isinstance(_data, DataFrame) else _data

    # if is_scalar(series):
    #     series = [series]
    # support generator
    series = list(series)

    if len(series) == 0:
        return pandas.Categorical([])
    if all(pandas.isna(series)):
        return pandas.Categorical([NA] * len(series))
    n = min(n, len(series))
    return pandas.cut(series, n, labels=range(n))


@register_func(None, context=Context.EVAL)
def min_rank(series: Iterable[Any], na_last: str = "keep") -> Iterable[float]:
    """Rank the data using min method"""
    return _rank(series, na_last=na_last, method='min')

@register_func(None, context=Context.EVAL)
def dense_rank(series: Iterable[Any], na_last: str = "keep") -> Iterable[float]:
    """Rank the data using dense method"""
    return _rank(series, na_last=na_last, method='dense')

@register_func(None, context=Context.EVAL)
def percent_rank(
        series: Iterable[Any],
        na_last: str = "keep"
) -> Iterable[float]:
    """Rank the data using percent_rank method"""
    ranking = _rank(series, na_last, 'min', True)
    minrank = ranking.min()
    maxrank = ranking.max()
    ret = ranking.transform(lambda r: (r-minrank)/(maxrank-minrank))
    ret[ranking.isna()] = NA
    return ret

@register_func(None, context=Context.EVAL)
def cume_dist(series: Iterable[Any], na_last: str = "keep") -> Iterable[float]:
    """Rank the data using percent_rank method"""
    ranking = _rank(series, na_last, 'min')
    max_ranking = ranking.max()
    ret = ranking.transform(lambda r: ranking.le(r).sum() / max_ranking)
    ret[ranking.isna()] = NA
    return ret

def _rank(
        data: Iterable[Any],
        na_last: str,
        method: str,
        percent: bool = False
) -> Iterable[float]:
    """Rank the data"""
    if not isinstance(data, pandas.Series):
        data = pandas.Series(data)

    # ascending = not isinstance(data, DescSeries)
    ascending = True

    ret = data.rank(
        method=method,
        ascending=ascending,
        pct=percent,
        na_option=(
            'keep' if na_last == 'keep'
            else 'top' if not na_last
            else 'bottom'
        )
    )
    return ret
