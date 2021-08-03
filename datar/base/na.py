"""NA related constants or functions"""

from typing import Any, Iterable

import numpy
from pipda import register_func

from ..core.contexts import Context
from ..core.types import is_null, is_scalar
from ..core.defaults import NA_REPR
from ..core.utils import register_numpy_func_x

# pylint: disable=invalid-name
NA = numpy.nan
NaN = NA
Inf = numpy.inf

# Just for internal and testing uses
NA_character_ = NA_REPR
NA_integer_ = numpy.random.randint(numpy.iinfo(numpy.int32).max)
NA_real_ = numpy.nan
NA_compex_ = complex(NA_real_, NA_real_)


@register_func(None, context=Context.EVAL)
def is_na(x: Any) -> Iterable[bool]:
    """Test if a value is nullable or elements in the value is nullable

    Args:
        x: The value to test

    Returns:
        If `x` is scalar, returns a scalar bool. Otherwise, return an array
        of bools.
    """
    return is_null(x)


@register_func(None, context=Context.EVAL)
def any_na(x: Any, recursive: bool = False) -> bool:
    """Check if any element in the value is nullable

    Args:
        x: The value to check
        recursive: Whether do a recursive check

    Returns:
        True if any element is nullable otherwise False
    """
    if is_scalar(x):
        return is_na(x)

    from .logical import is_true

    if not recursive:
        return any(is_true(is_na(elem)) for elem in x)

    out = False
    for elem in x:
        if any_na(elem, recursive=True):
            return True
    return out

is_infinite = register_numpy_func_x(
    "is_infinite",
    "isinf",
    doc="""Check if a value or values are infinite numbers

    Args:
        x: The value to check

    Returns:
        True if the value is infinite, False otherwise
        For iterable values, returns the element-wise results
    """
)

is_finite = register_numpy_func_x(
    "is_finite",
    "isfinite",
    doc="""Check if a value or values are finite numbers

    Args:
        x: The value to check

    Returns:
        True if the value is finite, False otherwise
        For iterable values, returns the element-wise results
    """
)

is_nan = register_numpy_func_x(
    "is_nan",
    "isnan",
    doc="""Check if a value or values are NaNs

    Args:
        x: The value to check

    Returns:
        True if the value is nan, False otherwise
        For iterable values, returns the element-wise results
    """
)
