"""Compute lagged or leading values

https://github.com/tidyverse/dplyr/blob/master/R/lead-lag.R
"""
# TODO: add tests
from typing import Iterable, Any, Optional

from pandas import Series
from pipda import register_func

from ..core.types import NumericType
from ..core.contexts import Context
from ..base.constants import NA

@register_func(None, context=Context.EVAL)
def lead(
        series: Iterable[Any],
        n: bool = 1,
        default: Any = NA,
        order_by: Optional[Iterable[NumericType]] = None
) -> Series:
    """Find next values in a vector

    Args:
        series: Vector of values
        n: Positive integer of length 1, giving the number of positions to
            lead or lag by
        default: Value used for non-existent rows.
        order_by: Override the default ordering to use another vector or column

    Returns:
        Lead or lag values with default values filled to series.
    """
    if not isinstance(series, Series):
        series = Series(series)

    index = series.index
    if order_by is not None:
        if not isinstance(order_by, Series):
            order_by = Series(order_by)
        order_by = order_by.sort_values()
        series = series.loc[order_by.index, :]

    ret = [default] * len(series)
    ret[:-n] = series.values[n:]
    if order_by is not None:
        ret = Series(ret, index=order_by.index)
        return ret.loc[index, :]
    return Series(ret, index=index)

@register_func(None, context=Context.EVAL)
def lag(
        series: Iterable[Any],
        n: bool = 1,
        default: Any = NA,
        order_by: Optional[Iterable[NumericType]] = None
) -> Series:
    """Find previous values in a vector

    See lead()
    """
    if not isinstance(series, Series):
        series = Series(series)

    index = series.index
    if order_by is not None:
        if not isinstance(order_by, Series):
            order_by = Series(order_by)
        order_by = order_by.sort_values()
        series = series.loc[order_by.index, :]

    ret = [default] * len(series)
    ret[n:] = series.values[:-n]
    if order_by is not None:
        ret = Series(ret, index=order_by.index)
        return ret.loc[index, :]
    return Series(ret, index=index)
