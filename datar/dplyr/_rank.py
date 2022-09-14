from types import GeneratorType
from functools import singledispatch

import numpy as np

from ..core.backends import pandas as pd
from ..core.backends.pandas import Categorical, DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar
from ..core.backends.pandas.core.groupby import GroupBy, SeriesGroupBy
from ..core.backends.pandas.core.generic import NDFrame

from ..core.tibble import TibbleGrouped


@singledispatch
def _rank(
    data,
    na_last,
    method,
    percent=False,
):
    """Rank the data"""
    is_series = isinstance(data, Series)

    if not is_series:
        data = Series(data)

    out = data.rank(
        method=method,
        pct=percent,
        na_option=(
            "keep" if na_last == "keep" else "top" if not na_last else "bottom"
        ),
    )

    return out if is_series else out.values


@_rank.register(GroupBy)
def _(
    data: GroupBy,
    na_last,
    method,
    percent=False,
):
    out = data.rank(
        method=method,
        pct=percent,
        na_option=(
            "keep" if na_last == "keep" else "top" if not na_last else "bottom"
        ),
    )
    return out


@singledispatch
def _row_number(x):
    return _rank(x, na_last="keep", method="first")


@_row_number.register(SeriesGroupBy)
def _(x):
    return x.transform(_row_number)


@_row_number.register(TibbleGrouped)
def _(x):
    grouped = x._datar["grouped"]
    return _row_number(
        Series(np.arange(x.shape[0]), index=x.index).groupby(
            grouped.grouper,
            observed=grouped.observed,
            sort=grouped.sort,
            dropna=grouped.dropna,
        )
    )


@_row_number.register(NDFrame)
def _(x):
    if x.ndim > 1:
        if x.shape[1] == 0:
            return []
        x = Series(np.arange(x.shape[0]), index=x.index)
    return _rank(x, na_last="keep", method="first")


@singledispatch
def _ntile(x, n):
    if is_scalar(x):
        x = [x]

    return _ntile(np.array(x), n)


@_ntile.register(GeneratorType)
def _(x, n):
    return _ntile(np.array(list(x)), n)


@_ntile.register(Series)
def _(x, n):
    return Series(_ntile(x.values, n), index=x.index, name=x.name)


@_ntile.register(DataFrame)
def _(x, n):
    x = _row_number(x)
    return _ntile(x, n)


@_ntile.register(TibbleGrouped)
def _(x, n):
    grouped = x._datar["grouped"]
    if x.shape[1] == 0:  # pragma: no cover
        x = _row_number(grouped)
    else:
        x = x[x.columns[0]]
    return _ntile(x, n)


@_ntile.register(np.ndarray)
def _(x, n):
    if x.size == 0:
        return Categorical([])

    if pd.isnull(x).all():
        return Categorical([np.nan] * x.size)

    n = min(n, x.size)
    return pd.cut(x, n, labels=np.arange(n) + 1)


@_ntile.register(GroupBy)
def _(x, n):
    return x.transform(
        lambda grup: pd.cut(
            grup,
            min(n, len(grup)),
            labels=np.arange(min(n, len(grup))) + 1,
        )
    )


@singledispatch
def _percent_rank(x, na_last="keep"):
    if len(x) == 0:
        dtype = getattr(x, "dtype", None)
        return np.array(x, dtype=dtype)

    return _percent_rank(Series(x), na_last).values


@_percent_rank.register(NDFrame)
def _(x, na_last="keep"):
    ranking = _rank(x, na_last, "min", True)
    minrank = ranking.min()
    maxrank = ranking.max()

    ret = (ranking - minrank) / (maxrank - minrank)
    ret[pd.isnull(ranking)] = np.nan
    return ret


@_percent_rank.register(GroupBy)
def _(x, na_last="keep"):
    ranking = _rank(x, na_last, "min", True).groupby(
        x.grouper,
        observed=x.observed,
        sort=x.sort,
        dropna=x.dropna,
    )
    maxs = ranking.transform("max")
    mins = ranking.transform("min")
    ret = ranking.transform(lambda r: (r - mins) / (maxs - mins))
    ret[ranking.obj.isna()] = np.nan
    return ret


@singledispatch
def _cume_dist(x, na_last="keep"):
    if is_scalar(x):
        x = [x]

    if len(x) == 0:
        dtype = getattr(x, "dtype", None)
        return np.array(x, dtype=dtype)

    return _cume_dist(Series(x), na_last).values


@_cume_dist.register(NDFrame)
def _(x, na_last="keep"):
    ranking = _rank(x, na_last, "min")
    total = (~pd.isnull(ranking)).sum()
    ret = ranking.transform(lambda r: ranking.le(r).sum() / total)
    ret[pd.isnull(ranking).values] = np.nan
    return ret


@_cume_dist.register(GroupBy)
def _(x, na_last="keep"):
    return x.transform(_cume_dist.dispatch(Series), na_last=na_last)
