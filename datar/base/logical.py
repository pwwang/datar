"""Logical/Boolean functions"""
import numpy as np
from pipda import register_func

from ..core.backends.pandas.api.types import is_scalar, is_bool_dtype
from ..core.contexts import Context

from .testing import _register_type_testing
from .casting import _as_type

TRUE = True
FALSE = False


@register_func(context=Context.EVAL)
def as_logical(x, na=None):
    """Convert an object or elements of an iterable into bool

    Args:
        x: The object

    Returns:
        When x is an array or a series, return x.astype(bool).
        When x is iterable, convert elements of it into bools
        Otherwise, convert x to bool.
    """
    return _as_type(x, bool, na=na)


as_bool = as_logical


is_logical = _register_type_testing(
    "is_logical",
    scalar_types=(bool, np.bool_),
    dtype_checker=is_bool_dtype,
    doc="""Test if a value is booleans

    Args:
        x: The value to be checked

    Returns:
        True if the value is an boolean or booleans; False otherwise.
    """,
)

is_bool = is_logical


@register_func(context=Context.EVAL)
def is_true(x):
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


@register_func(context=Context.EVAL)
def is_false(x):
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
