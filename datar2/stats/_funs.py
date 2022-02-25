from typing import Sequence
from functools import singledispatch

import numpy as np
from pandas import Series
from pandas.core.groupby import SeriesGroupBy

from ..core.tibble import TibbleRowwise
from ..base._arithmetic import _warn_na_rm


@singledispatch
def _quantile(
    x,
    probs: Sequence[float] = (0.0, 0.25, 0.5, 0.75, 1.0),
    na_rm: bool = True,
) -> np.ndarray:
    fun = np.nanquantile if na_rm else np.quantile
    return fun(x, probs)


@_quantile.register(TibbleRowwise)
def _(
    x: TibbleRowwise,
    probs: Sequence[float] = (0.0, 0.25, 0.5, 0.75, 1.0),
    na_rm: bool = True,
) -> Series:
    _warn_na_rm(
        "quantile",
        na_rm,
        "`na_rm` for rowwise is not supported and will always be True."
    )
    if len(probs) != 1:
        raise ValueError(
            "Values from `quantile(...)` with more than 1 `probs` "
            "cannot be recycled."
        )

    return x.quantile(probs, axis=1).T


@_quantile.register(SeriesGroupBy)
def _(
    x: SeriesGroupBy,
    probs: Sequence[float] = (0.0, 0.25, 0.5, 0.75, 1.0),
    na_rm: bool = True,
) -> Series:
    _warn_na_rm(
        "quantile",
        na_rm,
        "`na_rm` for grouped data is not supported and will always be True."
    )
    return x.quantile(probs).droplevel(-1)


@singledispatch
def _sd(
    x,
    na_rm: bool = True,
    ddof: int = 1,
) -> np.ndarray:
    fun = np.nanstd if na_rm else np.std
    return fun(x, ddof=ddof)


@_sd.register(TibbleRowwise)
def _(
    x: TibbleRowwise,
    na_rm: bool = True,
    ddof: int = 1,
) -> Series:
    return x.std(skipna=na_rm, ddof=ddof, axis=1)


@_sd.register(SeriesGroupBy)
def _(
    x: SeriesGroupBy,
    na_rm: bool = True,
    ddof: int = 1,
) -> Series:
    _warn_na_rm(
        "sd",
        na_rm,
        "`na_rm` for grouped data is not supported and will always be True."
    )
    return x.std(ddof)


@singledispatch
def _weighted_mean(
    x: Sequence,
    w: Sequence = None,
    na_rm: bool = True,
) -> np.ndarray:
    _warn_na_rm(
        "weighted_mean",
        na_rm,
        "`na_rm` is not supported"
    )
    if w is not None and np.sum(w) == 0:
        return np.nan

    return np.average(x, weights=w)


@_weighted_mean.register(TibbleRowwise)
def _(
    x: TibbleRowwise,
    w: Sequence = None,
    na_rm: bool = True,
) -> Series:
    _warn_na_rm(
        "weighted_mean",
        na_rm,
        "`na_rm` is not supported"
    )
    w = getattr(w, "obj", w)
    return x.apply(np.average, axis=1, weights=w)


@_weighted_mean.register(SeriesGroupBy)
def _(
    x: Sequence,
    w: Sequence = None,
    na_rm: bool = True,
) -> Series:
    _warn_na_rm(
        "weighted_mean",
        na_rm,
        "`na_rm` is not supported"
    )
    w = getattr(w, "obj", w)
    return x.apply(np.average, axis=1, kwargs={"weights": w})
