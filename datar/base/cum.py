"""Cumulative functions"""
from typing import Any

import numpy
from pandas import Series
from pipda import register_func

from ..core.utils import register_numpy_func_x, Array
from ..core.contexts import Context
from ..core.types import is_scalar, is_null

from .na import NA


def _ensure_nas_after_na(x: Any) -> Any:
    """Ensure NAs after first NA

    Since `cum<x>()` in R produces NAs after any NA appearance, but
    numpy/pandas just ignores the NAs and continue producing real values.
    """
    if is_scalar(x):
        return x
    na_indexes = numpy.flatnonzero(is_null(x))
    if len(na_indexes) == 0:
        return x
    na_index = na_indexes[0]
    x = Array(x)
    x[na_index:] = NA
    return x


# pylint: disable=invalid-name
cumsum = register_numpy_func_x(
    "cumsum",
    "cumsum",
    trans_in=_ensure_nas_after_na,
    doc="""Cumulative sum of elements.

Args:
    x: Input array

Returns:
    An array of cumulative sum of elements in x
""",
)

cumprod = register_numpy_func_x(
    "cumprod",
    "cumprod",
    trans_in=_ensure_nas_after_na,
    doc="""Cumulative product of elements.

Args:
    x: Input array

Returns:
    An array of cumulative product of elements in x
""",
)


@register_func(None, context=Context.EVAL)
def cummin(x: Any) -> Any:
    """Cummulative min along elements in x

    Args:
        x: Input array

    Returns:
        An array of cumulative min of elements in x
    """
    if is_scalar(x):
        return x
    x = _ensure_nas_after_na(x)
    return Series(x).cummin()


@register_func(None, context=Context.EVAL)
def cummax(x: Any) -> Any:
    """Cummulative max along elements in x

    Args:
        x: Input array

    Returns:
        An array of cumulative max of elements in x
    """
    if is_scalar(x):
        return x
    x = _ensure_nas_after_na(x)
    return Series(x).cummax()
