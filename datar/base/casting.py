"""Cast values between types"""

from typing import Any

import numpy
from pipda import register_func

from ..core.types import (
    Dtype, NumericOrIter, DoubleOrIter, IntOrIter,
    is_scalar, is_categorical
)
from ..core.contexts import Context
from ..core.utils import categorized

# pylint: disable=invalid-name

def _as_type(x: Any, type_: Dtype) -> Any:
    """Convert x or elements of x to certain type"""
    if hasattr(x, 'astype'):
        return x.astype(type_)

    if is_scalar(x):
        return type_(x)

    return type(x)(map(type_, x))


@register_func(None, context=Context.EVAL)
def as_double(x: Any) -> DoubleOrIter:
    """Convert an object or elements of an iterable into double/float

    Args:
        x: The object

    Returns:
        When x is an array or a series, return x.astype(float).
        When x is iterable, convert elements of it into floats
        Otherwise, convert x to float.
    """
    return _as_type(x, numpy.double)


@register_func(None, context=Context.EVAL)
def as_int(x: Any) -> IntOrIter:
    """Convert an object or elements of an iterable into int

    Args:
        x: The object

    Returns:
        When x is an array or a series, return x.astype(int).
        When x is iterable, convert elements of it into ints
        Otherwise, convert x to int.
    """
    if is_categorical(x):
        return categorized(x).codes
    return _as_type(x, int)

@register_func(None, context=Context.EVAL)
def as_integer(x: Any) -> IntOrIter:
    """Convert an object or elements of an iterable into int64

    Args:
        x: The object

    Returns:
        When x is an array or a series, return x.astype(numpy.int64).
        When x is iterable, convert elements of it into numpy.int64s
        Otherwise, convert x to numpy.int64.
    """
    if is_categorical(x):
        return categorized(x).codes
    return _as_type(x, numpy.int64)

@register_func(None, context=Context.EVAL)
def as_numeric(x: Any) -> NumericOrIter:
    """Make elements numeric"""
    try:
        return as_integer(x)
    except (ValueError, TypeError):
        return as_double(x)
