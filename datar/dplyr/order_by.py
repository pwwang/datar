"""Helper functions for ordering window function output

https://github.com/tidyverse/dplyr/blob/master/R/order-by.R
"""
from typing import Callable, Iterable, Any, Sequence

from pandas import Series
from pipda import register_func

from ..core.contexts import Context
from ..core.types import is_scalar


@register_func(None, context=Context.EVAL)
def order_by(
    order: Sequence[Any],
    data: Sequence[Any],
) -> Series:
    """Order the data by the given order

    Note:
        This behaves differently than `dplyr::order_by`, since we are unable
        to maintain the `data` argument to stay uncalled here.

        To control the order of the argument of a call, try `with_order`.

    Examples:
        >>> order_by(range(3,0), [5,4,3])
        >>> # [3,4,5]
        >>> order_by([5,4,3,2,1], cumsum([1,2,3,4,5]))
        >>> # 15,10,6,3,1
        >>> # instead of 15,14,12,9,5 from dplyr::order_by

    Args:
        `order`: An iterable to control the data order
        `data`: The data to be ordered

    Returns:
        The odered series. Note the original class of data will be lost.
    """
    if is_scalar(order):
        order = [order]
    if is_scalar(data):
        data = [data]

    order = Series(order) if len(order) > 1 else Series(order, dtype=object)
    order = order.sort_values()
    out = Series(data) if len(data) > 1 else Series(data, dtype=object)
    return out[order.index]


@register_func(None, context=Context.EVAL)
def with_order(
    order: Iterable[Any],
    func: Callable,
    x: Iterable[Any],
    *args: Any,
    **kwargs: Any,
) -> Series:
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
        A Seroes of ordered result
    """
    x = order_by(order, x)
    # expecting func to return an iterable
    out = func(x, *args, **kwargs)
    if isinstance(out, Series):
        # drop the index
        out = out.reset_index(drop=True)
    return order_by(order, out)
