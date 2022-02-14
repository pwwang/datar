from functools import singledispatch
from typing import Any, Iterable

import numpy
from pandas import Series
from pandas.core.groupby import SeriesGroupBy

from ..core.collections import Collection
from ..core.utils import all_groupby, logger, concat_groupby


def _warn_na_rm(funcname: str, na_rm: bool, extra_info: str = "") -> None:
    """Warn about na_rm False on SeriesGroupBy objects"""
    if not na_rm:
        logger.warning(
            "In %s(...): `na_rm` on SeriesGroupBy objects is always True. %s",
            funcname,
            extra_info,
        )


@singledispatch
def sum_internal(x, na_rm: bool = True):
    """Default way to sum"""
    func = numpy.nansum if na_rm else numpy.sum
    return func(x)


@sum_internal.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do sum on SeriesGroupBy object"""
    _warn_na_rm(
        "sum",
        na_rm,
        "Use f.x.sum(min_count=...) to control NA produces."
    )
    return x.sum()


@singledispatch
def prod_internal(x, na_rm: bool = True):
    """Default way to prod"""
    func = numpy.nanprod if na_rm else numpy.prod
    return func(x)


@prod_internal.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do prod on SeriesGroupBy object"""
    _warn_na_rm(
        "prod",
        na_rm,
        "Use f.x.prod(min_count=...) to control NA produces."
    )
    return x.prod()


@singledispatch
def mean_internal(x, na_rm: bool = True):
    """Default way to mean"""
    func = numpy.nanmean if na_rm else numpy.mean
    return func(x)


@mean_internal.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do mean on SeriesGroupBy object"""
    _warn_na_rm("mean", na_rm)
    return x.mean()


@singledispatch
def median_internal(x, na_rm: bool = True):
    """Default way to median"""
    func = numpy.nanmedian if na_rm else numpy.median
    return func(x)


@median_internal.register
def _(x: SeriesGroupBy, na_rm: bool = True) -> Series:
    """Do median on SeriesGroupBy object"""
    _warn_na_rm("median", na_rm)
    return x.median()


@singledispatch
def var_internal(x, na_rm: bool = True, ddof: int = 1):
    """Default way to do var"""
    func = numpy.nanvar if na_rm else numpy.var
    return func(x)


@var_internal.register
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
        return x[0].min()

    return concat_groupby(*x).min()


def min_internal(*x: Any, na_rm: bool = True):
    """Min of groups of values"""
    if all(isinstance(elem, SeriesGroupBy) for elem in x):
        return _min_grouped(*x)

    x = [elem.obj if isinstance(elem, SeriesGroupBy) else elem for elem in x]
    func = numpy.nanmin if na_rm else numpy.min
    if len(x) > 0:
        x = Collection(*x)  # flatten
    if len(x) == 0:
        logger.warning("In min(...): no non-missing arguments, returning inf")
        return numpy.inf
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


def max_internal(*x: Any, na_rm: bool = True):
    """Max of groups of values"""
    if all(isinstance(elem, SeriesGroupBy) for elem in x):
        return _max_grouped(*x)

    x = [elem.obj if isinstance(elem, SeriesGroupBy) else elem for elem in x]
    func = numpy.nanmax if na_rm else numpy.max
    if len(x) > 0:
        x = Collection(*x)  # flatten
    if len(x) == 0:
        logger.warning("In max(...): no non-missing arguments, returning -inf")
        return -numpy.inf
    return func(x)


# pmin/pmax
def pmin_internal(*x: Any, na_rm: bool = True) -> Iterable:
    """Do pmin internally"""
    if all_groupby(*x):
        return _pmin_grouped(*x, na_rm)

    df = tibble(*x)

