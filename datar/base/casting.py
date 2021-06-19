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
        Values converted into numpy.float64
    """
    return _as_type(x, numpy.double)

@register_func(None, context=Context.EVAL)
def as_float(x: Any, float_dtype: Dtype = numpy.float_) -> DoubleOrIter:
    """Convert an object or elements of an iterable into double/float

    Args:
        x: The object

    Returns:
        Converted value according to the float dtype
    """
    return _as_type(x, float_dtype)

@register_func(None, context=Context.EVAL)
def as_integer(x: Any, integer_dtype: Dtype = numpy.int_) -> IntOrIter:
    """Convert an object or elements of an iterable into int64

    Alias `as_int`

    Args:
        x: The object
        integer_dtype: The dtype of the integer. Could be one of:
            - `numpy.int0`
            - `numpy.int16`
            - `numpy.int32`
            - `numpy.int64`
            - `numpy.int8`
            - `numpy.int_`
            - `numpy.intc`
            - `numpy.intp`

    Returns:
        Converted values according to the integer_dtype
    """
    if is_categorical(x):
        return categorized(x).codes
    return _as_type(x, integer_dtype)

as_int = as_integer

@register_func(None, context=Context.EVAL)
def as_numeric(x: Any) -> NumericOrIter:
    """Make elements numeric

    Args:
        x: The value to convert

    Returns:
        Try `as_integer()` if failed then `as_float()`
    """
    try:
        return as_integer(x)
    except (ValueError, TypeError):
        return as_float(x)
