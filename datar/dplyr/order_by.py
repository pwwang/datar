"""Helper functions for ordering window function output

https://github.com/tidyverse/dplyr/blob/master/R/order-by.R
"""
from pipda.function import FastEvalFunction, Function
from pipda.utils import CallingEnvs

from ..base import order as order_fun


def order_by(order, call):
    """Order the data by the given order

    Note:
        This function should be called as an argument
        of a verb. If you want to call it regularly, try `with_order()`

    Examples:
        >>> df = tibble(x=f[1:6])
        >>> df >> mutate(y=order_by(f[5:], cumsum(f.x)))
        >>> # df.y:
        >>> # 15, 14, 12, 9, 5

    Args:
        order: An iterable to control the data order
        data: The data to be ordered

    Returns:
        A Function expression for verb to evaluate.
    """
    from ..datar import itemgetter

    order = order_fun(order, __calling_env=CallingEnvs.PIPING)
    if not isinstance(call, Function) or len(call._pipda_args) < 1:
        raise ValueError(
            "In `order_by()`: `call` must be a registered "
            f"function call with data, not `{type(call).__name__}`. \n"
            "            This function should be called as an argument "
            "of a verb. If you want to call it regularly, try `with_order()`"
        )

    x = itemgetter(call._pipda_args[0], order, __calling_env=CallingEnvs.PIPING)
    call._pipda_args = (x, *call._pipda_args[1:])
    return FastEvalFunction(itemgetter, (call, order), {}, dataarg=False)


def with_order(order, func, x, *args, **kwargs):
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
    expr = order_by(
        order,
        FastEvalFunction(func, (x, *args), kwargs, dataarg=False)
    )
    return expr._pipda_fast_eval()
