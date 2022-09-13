"""Compute lagged or leading values

https://github.com/tidyverse/dplyr/blob/master/R/lead-lag.R
"""
import numpy as np

from datar.core.factory import func_factory

from ..core.backends.pandas import Series
from ..core.backends.pandas.api.types import is_scalar

from .order_by import with_order


def _shift(x, n, default=None, order_by=None):
    if not isinstance(n, int):
        raise ValueError("`lead-lag` expect an integer for `n`.")

    if not is_scalar(default) and len(default) > 1:
        raise ValueError("`lead-lag` Expect scalar or length-1 `default`.")

    if not is_scalar(default):
        default = default[0]

    if order_by is not None:
        # newx = newx.reset_index(drop=True)
        out = with_order(order_by, Series.shift, x, n, fill_value=default)
    else:
        out = x.shift(n, fill_value=default)

    return out


@func_factory(kind="transform")
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


@func_factory(kind="transform")
def lag(x, n=1, default=np.nan, order_by=None):
    """Find previous values in a vector

    See lead()
    """
    return _shift(x, n=n, default=default, order_by=order_by)
