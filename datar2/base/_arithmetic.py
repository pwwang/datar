from functools import singledispatch
from typing import Any, Iterable

import numpy as np
from pandas.api.types import is_scalar
from pandas.core.series import Series
from pandas.core.groupby import SeriesGroupBy

from ..core.collections import Collection
from ..core.tibble import TibbleRowwise
# from ..core.utils import all_groupby, logger, concat_groupby
from ..core.utils import logger


def _warn_na_rm(funcname: str, na_rm: bool, extra_info: str = "") -> None:
    """Warn about na_rm False on SeriesGroupBy objects"""
    if not na_rm:
        logger.warning(
            "In %s(...): `na_rm` on SeriesGroupBy objects is always True. %s",
            funcname,
            extra_info,
        )


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
    """Do sum on SeriesGroupBy object"""
    _warn_na_rm(
        "sum",
        na_rm,
        "Use f.x.sum(min_count=...) to control NA produces."
    )
    return x.sum(axis=1)


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
    _warn_na_rm("mean", na_rm)
    return x.mean(axis=1)


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
def _min_grouped(*x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Get the min values of grouped serieses"""
    _warn_na_rm(
        "min",
        na_rm,
        "Use f.x.min(min_count=...) to control NA produces.",
    )

    if len(x) == 1:
        return x[0] if is_scalar(x[0]) else x[0].min()

    return concat_groupby(*x).min()


def _min(*x: Any, na_rm: bool = True):
    """Min of groups of values"""
    if all(is_scalar(elem) or isinstance(elem, SeriesGroupBy) for elem in x):
        return _min_grouped(*x)

    x = [elem.obj if isinstance(elem, SeriesGroupBy) else elem for elem in x]
    func = np.nanmin if na_rm else np.min
    if len(x) > 0:
        x = Collection(*x)  # flatten
    if len(x) == 0:
        logger.warning("In min(...): no non-missing arguments, returning inf")
        return np.inf
    return func(x)


def _max_grouped(*x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Get the max values of grouped serieses"""
    _warn_na_rm(
        "max",
        na_rm,
        "Use f.x.max(min_count=...) to control NA produces.",
    )

    if len(x) == 1:
        return x[0].max()

    return concat_groupby(*x).max()


def _max(*x: Any, na_rm: bool = True):
    """Max of groups of values"""
    if all(is_scalar(elem) or isinstance(elem, SeriesGroupBy) for elem in x):
        return _max_grouped(*x)

    x = [elem.obj if isinstance(elem, SeriesGroupBy) else elem for elem in x]
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
    if all_groupby(*x):
        return _pmin_grouped(*x, na_rm)

    df = tibble(*x)
