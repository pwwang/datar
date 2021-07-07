"""Functions from R-dplyr"""
from typing import Any, Iterable

import numpy
from pandas import DataFrame, Series
from pipda import register_func

from ..core.types import (
    BoolOrIter,
    NumericOrIter,
    NumericType,
    is_iterable,
    is_scalar,
    is_null,
)
from ..core.contexts import Context
from ..core.utils import position_at, Array
from ..base import NA


@register_func(None, context=Context.EVAL)
def between(
    x: NumericOrIter, left: NumericType, right: NumericType
) -> BoolOrIter:
    """Function version of `left <= x <= right`, works for both scalar and
    vector data

    See https://dplyr.tidyverse.org/reference/between.html

    Args:
        x: The data to test
        left: and
        right: The boundary values (must be scalars)

    Returns:
        A bool value of `x` is scalar, otherwise a `Series` of boolean values
    """
    if not is_scalar(left) or not is_scalar(right):
        raise ValueError(f"`{between}` expects scalars for `left` and `right`.")
    if is_scalar(x):
        if is_null([x, left, right]).any():
            return NA
        return left <= x <= right
    return Series(between(elem, left, right) for elem in x)


@register_func(None, context=Context.EVAL)
def cummean(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative means"""
    if not isinstance(series, Series):
        series = Series(series, dtype="float64")
    if len(series) > 0:
        return series.cumsum(skipna=False) / (Series(range(len(series))) + 1.0)
    return Series([], dtype="float64")


@register_func(None, context=Context.EVAL)
def cumall(series: Any) -> Series:
    """Get cumulative bool. All cases after first False"""
    if is_scalar(series):
        series = [series]

    bools = boolean(series)  # all to bool, with NAs kept
    out = []
    out_append = out.append
    for elem in bools:
        if not out:
            out_append(elem)
        elif out[-1] is True and elem is True:
            out_append(True)
        elif out[-1] is False or elem is False:
            out_append(False)
        else:
            out_append(NA)
    return Series(out) if out else Series([], dtype=bool)


@register_func(None, context=Context.EVAL)
def cumany(series: Any) -> Series:
    """Get cumulative bool. All cases after first True"""
    if is_scalar(series):
        series = [series]

    # numpy treats NA differently than R does
    bools = boolean(series)  # all to bool, with NAs kept
    out = []
    out_append = out.append
    for elem in bools:
        if not out:
            out_append(elem)
        elif out[-1] is True or elem is True:
            out_append(True)
        else:
            out_append(NA)
    # let itself choose dtype (bool or object if NA exists)
    return Series(out) if out else Series([], dtype=bool)


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
    if not replace:
        return x

    if isinstance(x, DataFrame):
        y = x.copy()
        for repl in replace:
            x = y.combine_first(repl)
            y = x
        return y

    if is_iterable(x):
        x = Series(x)
        for repl in replace:
            x = x.combine_first(
                Series(repl if is_iterable(repl) else [repl] * len(x))
            )
        return x.values

    return replace[0] if is_null(x) else x


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
        x = Series(x, dtype=object) if len(x) == 0 else Series(x)

    x = x.copy()
    x[x == y] = NA
    return x


@register_func(None, context=Context.EVAL)
def near(x: Iterable[Any], y: Any, tol: float = 1e-8) -> Iterable[bool]:
    """Compare numbers with tolerance"""
    if is_scalar(x):
        x = [x]

    return numpy.isclose(x, y, atol=tol)


@register_func(None, context=Context.EVAL)
def nth(
    x: Iterable[Any],
    n: int,
    order_by: Iterable[Any] = None,
    default: Any = NA,
    base0_: bool = None,
) -> Any:
    """Get the nth element of x

    See https://dplyr.tidyverse.org/reference/nth.html

    Args:
        x: A collection of elements
        n: The order of the elements.
        order_by: An optional vector used to determine the order
        default: A default value to use if the position does not exist
            in the input.
        base0_: Whether `n` is 0-based or not.

    Returns:
        A single element of x at `n'th`
    """
    x = Array(x)
    if order_by is not None:
        order_by = Array(order_by)
        x = x[order_by.argsort()]
    if not isinstance(n, int):
        raise TypeError("`nth` expects `n` to be an integer")

    try:
        return x[position_at(n, len(x), base0=base0_)]
    except (ValueError, IndexError, TypeError):
        return default


@register_func(None, context=Context.EVAL)
def first(
    x: Iterable[Any],
    order_by: Iterable[Any] = None,
    default: Any = NA,
) -> Any:
    """Get the first element of x"""
    x = Array(x)
    if order_by is not None:
        order_by = Array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[0]
    except IndexError:
        return default


@register_func(None, context=Context.EVAL)
def last(
    x: Iterable[Any],
    order_by: Iterable[Any] = None,
    default: Any = NA,
) -> Any:
    """Get the last element of x"""
    x = Array(x)
    if order_by is not None:
        order_by = Array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[-1]
    except IndexError:
        return default


def boolean(value: Any) -> BoolOrIter:
    """Convert value to bool, but keep NAs"""
    if is_scalar(value):
        return NA if is_null(value) else bool(value)
    return [boolean(elem) for elem in value]
