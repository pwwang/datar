from types import GeneratorType
from functools import singledispatch

import numpy as np
import pandas as pd
from pandas import Categorical, Series
from pandas.api.types import is_scalar
from pandas.core.groupby import GroupBy
from pandas.core.generic import NDFrame

from ..core.tibble import TibbleGrouped


@singledispatch
def _rank(
    data,
    na_last,
    method,
    percent=False,
):
    """Rank the data"""
    if not isinstance(data, Series):
        data = Series(data)

    out = data.rank(
        method=method,
        pct=percent,
        na_option=(
            "keep" if na_last == "keep" else "top" if not na_last else "bottom"
        ),
    )

    return out


@_rank.register(GroupBy)
def _(
    data: GroupBy,
    na_last,
    method,
    percent=False,
):
    return data.rank(
        method=method,
        pct=percent,
        na_option=(
            "keep" if na_last == "keep" else "top" if not na_last else "bottom"
        ),
    )


@singledispatch
def _row_number(x):
    return _rank(x, na_last="keep", method="first").values


@_row_number.register(GroupBy)
def _(x):
    return x.cumcount() + 1


@_row_number.register(TibbleGrouped)
def _(x):
    return _row_number(x._datar["grouped"])


@_row_number.register(NDFrame)
def _(x):
    dtype = object if x.shape[0] == 0 else None
    return Series(range(1, x.shape[0] + 1), index=x.index, dtype=dtype)


@singledispatch
def _ntile(x, n):
    if is_scalar(x):
        x = [x]

    return _ntile(np.array(x), n)


@_ntile.register(GeneratorType)
def _(x, n):
    return _ntile(np.array(list(x)), n)


@_ntile.register(NDFrame)
def _(x, n):
    return _ntile(x.values, n)


@_ntile.register(np.ndarray)
def _(x, n):
    if x.size == 0:
        return Categorical([])

    if pd.isnull(x).all():
        return Categorical([np.nan] * x.size)

    n = min(n, x.size)
    return pd.cut(x, n, labels=range(n))


@_ntile.register(GroupBy)
def _(x, n):
    return x.transform(
        lambda grup: pd.cut(
            grup,
            min(n, len(grup)),
            labels=range(min(n, len(grup))),
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
    ranking = _rank(x, na_last, "min", True).groupby(x.grouper)
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
    total = x.shape[0]
    ret = ranking.transform(lambda r: ranking.le(r).sum() / total)
    ret[pd.isnull(ranking)] = np.nan
    return ret


@_cume_dist.register(GroupBy)
def _(x, na_last="keep"):
    ranking = _rank(x, na_last, "min").groupby(x.grouper)
    # faster way?
    return (
        ranking.apply(
            lambda ranks: (
                np.array(
                    [
                        np.nan if pd.isnull(r) else (ranks <= r).sum()
                        for r in ranks
                    ]
                )
                / ranks.size
            )
        )
        .explode()
        .astype(float)
    )
