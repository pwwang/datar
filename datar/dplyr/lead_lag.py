"""Compute lagged or leading values

https://github.com/tidyverse/dplyr/blob/master/R/lead-lag.R
"""
from typing import Iterable, Any, Optional

from pandas import Series, Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from pipda import register_func

from ..core.types import NumericType, is_scalar
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
    if n == 0: # ignore other arguments
        return series

    series, cats, default = lead_lag_prepare(series, n, default, order_by)
    index = series.index

    ret = default * len(series)
    ret[:-n] = series.values[n:]
    if order_by is not None:
        ret = Series(ret, index=order_by.index)
        ret = ret[index]
    if cats is not None:
        ret = Categorical(ret, categories=cats)
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
    if n == 0: # ignore other arguments
        return series

    series, cats, default = lead_lag_prepare(series, n, default, order_by)
    index = series.index

    ret = default * len(series)
    ret[n:] = series.values[:-n]
    if order_by is not None:
        ret = Series(ret, index=order_by.index)
        ret = ret[index]
    if cats is not None:
        ret = Categorical(ret, categories=cats)
    return Series(ret, index=index)

def lead_lag_prepare(
        data: Iterable[Any],
        n: int,
        default: Any,
        order_by: Optional[Iterable[Any]]
):
    """Prepare and check arguments for lead-lag"""
    cats = None
    if is_categorical_dtype(data):
        if isinstance(data, Series):
            data = data.cat
        cats = data.categories

    if not isinstance(n, int) or n < 0:
        raise ValueError(f"`lead-lag` expect a non-negative integer for `n`.")

    if is_scalar(default):
        default = [default]
    if len(default) != 1:
        raise ValueError(f"`lead-lag` Expect scalar or length-1 `default`.")

    if not isinstance(data, Series):
        data = Series(data)

    if order_by is not None:
        if not isinstance(order_by, Series):
            order_by = Series(order_by)
        order_by = order_by.sort_values()
        data = data[order_by.index]

    return data, cats, default