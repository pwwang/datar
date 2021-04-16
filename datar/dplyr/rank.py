"""Windowed rank functions.

See https://github.com/tidyverse/dplyr/blob/master/R/slice.R
"""
from typing import Any, Iterable

import numpy
import pandas
from pipda import register_func

from ..core.contexts import Context
from ..base.funcs import NA

@register_func(context=Context.EVAL)
def row_number(_data: Iterable[Any]) -> numpy.ndarray:
    """Gives the row number, 0-based."""
    return numpy.array(range(len(_data)))

@register_func(None, context=Context.EVAL)
def ntile(series: Iterable[Any], n: int) -> Iterable[Any]:
    """A rough rank, which breaks the input vector into ‘n’ buckets."""
    if len(series) == 0:
        return []
    if all(pandas.isna(ser) for ser in series):
        return [NA] * len(series)
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
