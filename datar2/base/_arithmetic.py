from functools import singledispatch
from typing import Any, Iterable

import numpy as np
from pandas.core.series import Series
from pandas.core.groupby import SeriesGroupBy

from ..core.collections import Collection
from ..core.tibble import Tibble, TibbleRowwise
from ..core.utils import logger


def _warn_na_rm(funcname: str, na_rm: bool, extra_info: str = "") -> None:
    """Warn about na_rm False on SeriesGroupBy objects"""
    if not na_rm:
        logger.warning(
            "In %s(...): `na_rm` on SeriesGroupBy objects is always True. %s",
            funcname,
            extra_info,
        )


def _all_elems(x) -> str:
    all_rowwise = True
    all_grouped = True
    for elem in x:
        if not getattr(elem, "is_rowwise", False):
            all_rowwise = False

        if not isinstance(elem, SeriesGroupBy):
            all_grouped = False

    if all_rowwise:
        return "rowwise"

    if all_grouped:
        return "grouped"

    return "other"


@singledispatch
def _sum(x, na_rm: bool = True):
    """Default way to sum"""
    func = np.nansum if na_rm else np.sum
    return func(x)


@_sum.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do sum on SeriesGroupBy object"""
    _warn_na_rm(
        "sum",
        na_rm,
        "Use f.x.sum(min_count=...) to control NA produces."
    )
    return x.sum()


@_sum.register
def _(x: TibbleRowwise, na_rm: bool = True) -> Series:
    """Do sum on TibbleRowwise object"""
    return x.sum(axis=1, skipna=na_rm)


@singledispatch
def _prod(x, na_rm: bool = True):
    """Default way to prod"""
    func = np.nanprod if na_rm else np.prod
    return func(x)


@_prod.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do prod on SeriesGroupBy object"""
    _warn_na_rm(
        "prod",
        na_rm,
        "Use f.x.prod(min_count=...) to control NA produces."
    )
    return x.prod()


@singledispatch
def _mean(x, na_rm: bool = True):
    """Default way to mean"""
    func = np.nanmean if na_rm else np.mean
    return func(x)


@_mean.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do mean on SeriesGroupBy object"""
    _warn_na_rm("mean", na_rm)
    return x.mean()


@_mean.register
def _(x: TibbleRowwise, na_rm: bool = True) -> Series:
    return x.mean(axis=1, skipna=na_rm)


@singledispatch
def _median(x, na_rm: bool = True):
    """Default way to median"""
    func = np.nanmedian if na_rm else np.median
    return func(x)


@_median.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do median on SeriesGroupBy object"""
    _warn_na_rm("median", na_rm)
    return x.median()


@singledispatch
def _var(x, na_rm: bool = True, ddof: int = 1):
    """Default way to do var"""
    func = np.nanvar if na_rm else np.var
    return func(x)


@_var.register
def _(x: SeriesGroupBy, na_rm: bool = True, ddof: int = 1) -> Series:
    """Do median on SeriesGroupBy object"""
    _warn_na_rm("var", na_rm)
    return x.var(ddof=ddof)


# min/max
def _min(*x: Any, na_rm: bool = True):
    """Min of groups of values"""
    xtype = _all_elems(x)
    if xtype == "rowwise":
        return Tibble.from_args(*x).min(axis=1, skipna=na_rm)

    _warn_na_rm(
        "min",
        na_rm,
        "Use f.x.min(min_count=...) to control NA produces.",
    )

    if xtype == "grouped":
        return Tibble.from_args(*x)._datar["grouped"].min()

    func = np.nanmin if na_rm else np.min
    if len(x) > 0:
        x = Collection(*x)  # flatten
    if len(x) == 0:
        logger.warning("In min(...): no non-missing arguments, returning inf")
        return np.inf
    return func(x)


def _max(*x: Any, na_rm: bool = True):
    """Max of groups of values"""
    xtype = _all_elems(x)
    if xtype == "rowwise":
        return Tibble.from_args(*x).max(axis=1, skipna=na_rm)

    _warn_na_rm(
        "max",
        na_rm,
        "Use f.x.max(max=...) to control NA produces.",
    )

    if xtype == "grouped":
        return Tibble.from_args(*x)._datar["grouped"].max()

    func = np.nanmax if na_rm else np.max
    if len(x) > 0:
        x = Collection(*x)  # flatten
    if len(x) == 0:
        logger.warning("In max(...): no non-missing arguments, returning -inf")
        return -np.inf
    return func(x)


# pmin/pmax
def _pmin(*x: Any, na_rm: bool = True) -> Iterable:
    """Do pmin internally"""
    return Tibble.from_args(*x).min(axis=1, skipna=na_rm)


def _pmax(*x: Any, na_rm: bool = True) -> Iterable:
    """Do pmax internally"""
    return Tibble.from_args(*x).max(axis=1, skipna=na_rm)
