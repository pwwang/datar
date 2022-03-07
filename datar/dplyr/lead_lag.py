"""Compute lagged or leading values

https://github.com/tidyverse/dplyr/blob/master/R/lead-lag.R
"""
import numpy as np
from pandas import Series
from pandas.api.types import is_scalar
from pipda import register_func

from ..core.contexts import Context
from ..core.factory import dispatching
from .order_by import with_order


@dispatching(kind="transform", qualname="datar.dplyr.lead/lag")
def _shift(x, n, default=None, order_by=None):
    if not isinstance(n, int):
        raise ValueError("`lead-lag` expect an integer for `n`.")

    if not is_scalar(default) and len(default) > 1:
        raise ValueError("`lead-lag` Expect scalar or length-1 `default`.")

    if not is_scalar(default):
        default = default[0]

    newx = x
    if not isinstance(x, Series):
        newx = Series(x)

    if order_by is not None:
        newx = newx.reset_index(drop=True)
        out = with_order(order_by, Series.shift, newx, n, fill_value=default)
    else:
        out = newx.shift(n, fill_value=default)

    return out


@register_func(None, context=Context.EVAL)
def lead(x, n=1, default=np.nan, order_by=None):
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
    return _shift(x, n=-n, default=default, order_by=order_by)


@register_func(None, context=Context.EVAL)
def lag(x, n=1, default=np.nan, order_by=None):
    """Find previous values in a vector

    See lead()
    """
    return _shift(x, n=n, default=default, order_by=order_by)
