from functools import singledispatch

import numpy as np
from pipda import register_func

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame, Series, Categorical
from ..core.backends.pandas.api.types import is_scalar, is_integer
from ..core.backends.pandas.core.groupby import SeriesGroupBy

from ..core.contexts import Context
from ..core.tibble import TibbleGrouped, reconstruct_tibble
from ..core.utils import ensure_nparray, logger


def _rep(x, times, length, each):
    """Repeat sequence x"""
    x = ensure_nparray(x)
    times = ensure_nparray(times)
    length = ensure_nparray(length)
    each = ensure_nparray(each)
    if times.size == 1:
        times = times[0]
    if length.size >= 1:
        if length.size > 1:
            logger.warning(
                "In rep(...) : first element used of 'length' argument"
            )
        length = length[0]
    if each.size == 1:
        each = each[0]

    if not is_scalar(times):
        if times.size != x.size:
            raise ValueError(
                "Invalid times argument, expect length "
                f"{x.size}, got {times.size}"
            )

        if not is_integer(each) or each != 1:
            raise ValueError(
                "Unexpected each argument when times is an iterable."
            )

    if is_integer(times) and is_scalar(times):
        x = np.tile(np.repeat(x, each), times)
    else:
        x = np.repeat(x, times)

    if length is None:
        return x

    repeats = length // x.size + 1
    x = np.tile(x, repeats)

    return x[:length]


@singledispatch
def _rep_dispatched(x, times, length, each):
    """Repeat sequence x"""
    times_sgb = isinstance(times, SeriesGroupBy)
    length_sgb = isinstance(length, SeriesGroupBy)
    each_sgb = isinstance(each, SeriesGroupBy)
    values = {}
    if times_sgb:
        values["times"] = times
    if length_sgb:
        values["length"] = length
    if each_sgb:
        values["each"] = each

    if values:
        from ..tibble import tibble
        df = tibble(**values)
        out = df._datar["grouped"].apply(
            lambda subdf: _rep(
                x,
                times=subdf["times"] if times_sgb else times,
                length=subdf["length"] if length_sgb else length,
                each=subdf["each"] if each_sgb else each,
            )
        )
        non_na_out = out[out.transform(len) > 0]
        non_na_out = non_na_out.explode()
        grouping = Categorical(non_na_out.index, categories=out.index.unique())
        return (
            non_na_out.explode()
            .reset_index(drop=True)
            .groupby(
                grouping,
                observed=False,
                sort=df._datar["grouped"].sort,
                dropna=df._datar["grouped"].dropna,
            )
        )

    return _rep(x, times, length, each)


@_rep_dispatched.register(Series)
def _(x, times, length, each):
    return _rep_dispatched.dispatch(object)(x.values, times, length, each)


@_rep_dispatched.register(SeriesGroupBy)
def _(x, times, length, each):
    from ..tibble import tibble
    df = tibble(x=x)
    times_sgb = isinstance(times, SeriesGroupBy)
    length_sgb = isinstance(length, SeriesGroupBy)
    each_sgb = isinstance(each, SeriesGroupBy)
    if times_sgb:
        df["times"] = times
    if length_sgb:
        df["length"] = length
    if each_sgb:
        df["each"] = each

    out = df._datar["grouped"].apply(
        lambda subdf: _rep(
            subdf["x"],
            times=subdf["times"] if times_sgb else times,
            length=subdf["length"] if length_sgb else length,
            each=subdf["each"] if each_sgb else each,
        )
    ).explode().astype(x.obj.dtype)
    grouping = out.index
    return out.reset_index(drop=True).groupby(
        grouping,
        observed=df._datar["grouped"].observed,
        sort=df._datar["grouped"].sort,
        dropna=df._datar["grouped"].dropna,
    )


@_rep_dispatched.register(DataFrame)
def _(x, times, length, each):
    if not is_integer(each) or each != 1:
        raise ValueError(
            "`each` has to be 1 to replicate a data frame."
        )

    out = pd.concat([x] * times, ignore_index=True)
    if length is not None:
        out = out.iloc[:length, :]

    return out


@_rep_dispatched.register(TibbleGrouped)
def _(x, times, length, each):
    out = _rep_dispatched.dispatch(DataFrame)(x, times, length, each)
    return reconstruct_tibble(x, out)


@register_func(context=Context.EVAL)
def rep(
    x,
    times=1,
    length=None,
    each=1,
):
    """replicates the values in x

    Args:
        x: a vector or scaler
        times: number of times to repeat each element if of length len(x),
            or to repeat the whole vector if of length 1
        length: non-negative integer. The desired length of the output vector
        each: non-negative integer. Each element of x is repeated each times.

    Returns:
        An array of repeated elements in x.
    """
    return _rep_dispatched(x, times, length, each)
