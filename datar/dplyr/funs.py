"""Functions from R-dplyr"""
# TODO: add tests
from typing import Any, Iterable, Optional

import numpy
from pandas import DataFrame, Series
from pipda import register_func

from ..core.types import (
    BoolOrIter, NumericOrIter, NumericType,
    is_iterable, is_scalar
)
from ..core.utils import copy_flags
from ..core.contexts import Context
from ..base.constants import NA

@register_func(None, context=Context.EVAL)
def between(
        x: NumericOrIter,
        left: NumericType,
        right: NumericType
) -> BoolOrIter:
    """Function version of `left <= x <= right`, which cannot do it rowwisely
    """
    if is_scalar(x):
        return left <= x <= right
    return Series(between(elem, left, right) for elem in x)

@register_func(None, context=Context.EVAL)
def cummean(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative means"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cumsum(skipna=False) / (Series(range(len(series))) + 1.0)

@register_func(None, context=Context.EVAL)
def cumall(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative bool. All cases after first False"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummin(skipna=False).astype(bool)

@register_func(None, context=Context.EVAL)
def cumany(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative bool. All cases after first True"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummax(skipna=False).astype(bool)


@register_func(None, context=Context.EVAL)
def coalesce(x: Any, *replace: Any) -> Any:
    """Replace missing values

    https://dplyr.tidyverse.org/reference/coalesce.html

    Args:
        x: The vector to replace
        replace: The replacement

    Returns:
        A vector the same length as the first argument with missing values
        replaced by the first non-missing value.
    """
    # TODO: replace copy_flags with copy_attrs
    if not replace:
        return x

    if isinstance(x, DataFrame):
        y = x.copy()
        copy_flags(y, x)
        for repl in replace:
            x = y.combine_first(repl)
            copy_flags(x, y)
            y = x
        return y

    if is_iterable(x):
        x = Series(x)
        for repl in replace:
            x = x.combine_first(
                Series(repl if is_iterable(repl) else [repl] * len(x))
            )
        return x.values

    return replace[0] if numpy.isnan(x) else x

@register_func(None, context=Context.EVAL)
def na_if(x: Iterable[Any], y: Any) -> Iterable[Any]:
    """Convert an annoying value to NA

    Args:
        x: Vector to modify
        y: Value to replace with NA

    Returns:
        A vector with values replaced.
    """
    if is_scalar(x):
        x = [x]
    if not isinstance(x, Series):
        x = Series(x)

    if not isinstance(y, Series) and is_scalar(y):
        y = Series(y)
    if isinstance(y, Series):
        y = y.values

    x = x.to_frame(name='x')
    x.loc[x.x.values == y] = NA
    return x['x']

@register_func(None, context=Context.EVAL)
def near(x: Iterable[Any], y: Any, tol: float = 1e-8) -> Iterable[Any]:
    """Compare numbers with tolerance"""
    if is_scalar(x):
        x = [x]

    return numpy.isclose(x, y, atol=tol)

@register_func(None, context=Context.EVAL)
def nth(
        x: Iterable[Any],
        n: int,
        order_by: Optional[Iterable[Any]] = None,
        default: Any = NA
) -> Any:
    """Get the nth element of x"""
    x = numpy.array(x)
    if order_by is not None:
        order_by = numpy.array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[n]
    except IndexError:
        return default

@register_func(None, context=Context.EVAL)
def first(
        x: Iterable[Any],
        order_by: Optional[Iterable[Any]] = None,
        default: Any = NA
) -> Any:
    """Get the first element of x"""
    x = numpy.array(x)
    if order_by is not None:
        order_by = numpy.array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[0]
    except IndexError:
        return default

@register_func(None, context=Context.EVAL)
def last(
        x: Iterable[Any],
        order_by: Optional[Iterable[Any]] = None,
        default: Any = NA
) -> Any:
    """Get the last element of x"""
    x = numpy.array(x)
    if order_by is not None:
        order_by = numpy.array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[-1]
    except IndexError:
        return default
