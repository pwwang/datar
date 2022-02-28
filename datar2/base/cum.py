"""Cumulative functions"""
from functools import singledispatch
from typing import Any, Union

from pandas import Series
from pandas._typing import AnyArrayLike
from pandas.api.types import is_scalar
from pandas.core.generic import NDFrame
from pandas.core.groupby import GroupBy
from pipda import register_func

from ..core.utils import transform_func
from ..core.tibble import TibbleRowwise
from ..core.contexts import Context


cumsum = transform_func(
    "cumsum",
    doc="""Cumulative sum of elements.

    Args:
        x: Input array

    Returns:
        An array of cumulative sum of elements in x
    """,
)

cumprod = transform_func(
    "cumprod",
    doc="""Cumulative product of elements.

    Args:
        x: Input array

    Returns:
        An array of cumulative product of elements in x
    """,
)


@singledispatch
def _cummin(x) -> AnyArrayLike:
    if is_scalar(x):
        return x
    return Series(x).cummin().values


@_cummin.register(TibbleRowwise)
def _(x: TibbleRowwise) -> AnyArrayLike:
    return x.cummin(axis=1)


@_cummin.register(NDFrame)
@_cummin.register(GroupBy)
def _(x: Union[NDFrame, GroupBy]) -> AnyArrayLike:
    return x.cummin()


@singledispatch
def _cummax(x) -> AnyArrayLike:
    if is_scalar(x):
        return x
    return Series(x).cummax().values


@_cummax.register(TibbleRowwise)
def _(x: TibbleRowwise) -> AnyArrayLike:
    return x.cummax(axis=1)


@_cummax.register(NDFrame)
@_cummax.register(GroupBy)
def _(x: Union[NDFrame, GroupBy]) -> AnyArrayLike:
    return x.cummax()


@register_func(None, context=Context.EVAL)
def cummin(x: Any) -> Any:
    """Cummulative min along elements in x

    Note that in `R`, it will be all NA's after an NA appears, but pandas will
    ignore that NA value

    Args:
        x: Input array

    Returns:
        An array of cumulative min of elements in x
    """
    return _cummin(x)


@register_func(None, context=Context.EVAL)
def cummax(x: Any) -> Any:
    """Cummulative max along elements in x

    Note that in `R`, it will be all NA's after an NA appears, but pandas will
    ignore that NA value

    Args:
        x: Input array

    Returns:
        An array of cumulative max of elements in x
    """
    return _cummax(x)
