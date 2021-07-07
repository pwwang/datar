"""Logical/Boolean functions"""
from typing import Any

import numpy
from pandas.core.dtypes.common import is_bool_dtype
from pipda import register_func

from ..core.types import BoolOrIter, is_scalar
from ..core.contexts import Context

from .testing import _register_type_testing
from .casting import _as_type

TRUE = True
FALSE = False

# pylint: disable=invalid-name


@register_func(None, context=Context.EVAL)
def as_logical(x: Any) -> BoolOrIter:
    """Convert an object or elements of an iterable into bool

    Args:
        x: The object

    Returns:
        When x is an array or a series, return x.astype(bool).
        When x is iterable, convert elements of it into bools
        Otherwise, convert x to bool.
    """
    return _as_type(x, bool)


as_bool = as_logical


is_logical = _register_type_testing(
    "is_logical",
    scalar_types=(bool, numpy.bool_),
    dtype_checker=is_bool_dtype,
    doc="""Test if a value is booleans

Args:
    x: The value to be checked

Returns:
    True if the value is an boolean or booleans; False otherwise.
""",
)

is_bool = is_logical


@register_func(None, context=Context.EVAL)
def is_true(x: Any) -> bool:
    """Check if a value is a scalar True, like `isTRUE()` in `R`.

    If the value is non-scalar, will return False
    This does not require exact True, but a value that passes
    `if` test in python

    Args:
        x: The value to test

    Returns:
        True if x is True otherwise False
    """
    if not is_scalar(x):
        return False
    return bool(x)


@register_func(None, context=Context.EVAL)
def is_false(x: Any) -> bool:
    """Check if a value is a scalar False, like `isFALSE()` in `R`.

    If the value is non-scalar, will return False
    This does not require exact False, but a value that fails
    `if` test in python

    Args:
        x: The value to test

    Returns:
        False if x is False otherwise True
    """
    if not is_scalar(x):
        return False
    return not bool(x)
