"""Helper functions for ordering window function output

https://github.com/tidyverse/dplyr/blob/master/R/order-by.R
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence
from functools import singledispatch

import numpy as np
from pipda import FunctionCall

from ..core.factory import func_factory
from ..core.backends.pandas import Series, DataFrame
from ..core.backends.pandas.core.base import PandasObject

from ..base import order as order_fun


def order_by(order: Sequence, call: FunctionCall):
    """Order the data by the given order

    Note:
        This function should be called as an argument
        of a verb. If you want to call it regularly, try `with_order()`

    Examples:
        >>> df = tibble(x=c[1:6])
        >>> df >> mutate(y=order_by(c[5:], cumsum(f.x)))
        >>> # df.y:
        >>> # 15, 14, 12, 9, 5

    Args:
        order: An iterable to control the data order
        data: The data to be ordered

    Returns:
        A Function expression for verb to evaluate.
    """
    from ..datar import itemgetter

    order = order_fun(order)
    if not isinstance(call, FunctionCall) or len(call._pipda_args) < 1:
        raise ValueError(
            "In `order_by()`: `call` must be a registered "
            f"function call with data, not `{type(call).__name__}`. \n"
            "            This function should be called as an argument "
            "of a verb. If you want to call it regularly, try `with_order()`"
        )

    x = itemgetter(call._pipda_args[0], order)
    call._pipda_args = (x, *call._pipda_args[1:])
    return itemgetter(call, order)


@singledispatch
def _with_order(seq, order):
    return [seq[i] for i in order]


@_with_order.register(np.ndarray)
def _(seq, order):
    return seq.take(order)


@_with_order.register(Series)
def _(seq, order):
    out = seq.take(order)
    out.index = seq.index
    return out


@func_factory({'order', 'x'})
def with_order(
    order: Sequence,
    func: Callable,
    x: Any,
    *args: Any,
    __args_raw: Mapping[str, Any] = None,
    **kwargs: Any,
) -> Sequence:
    """Control argument and result of a window function

    Examples:
        >>> with_order([5,4,3,2,1], cumsum, [1,2,3,4,5])
        >>> # 15, 14, 12, 9, 5

    Args:
        order: An iterable to order the arugment and result
        func: The window function
        x: The first arugment for the function
        *args: and
        **kwargs: Other arugments for the function

    Returns:
        The ordered result or an expression if there is expression in arguments
    """
    order = order_fun(order)

    x = _with_order(x, order)
    out = func(x, *args, **kwargs)
    out = _with_order(out, order)

    return out


@with_order.register(DataFrame, func="default", post="decor")
def _with_order_post(__out, order, func, x, *args, __args_raw=None, **kwargs):
    """Keep the raw values if input is not Series-alike"""
    if (
        not isinstance(__args_raw["x"], PandasObject)
        and isinstance(__out, Series)
    ):
        return __out.values

    return __out
