"""Cast values between types"""

from typing import Any

import numpy
from pipda import register_func

from ..core.types import (
    Dtype,
    NumericOrIter,
    DoubleOrIter,
    IntOrIter,
    is_scalar,
    is_categorical,
)
from ..core.contexts import Context
from ..core.utils import categorized
from ..core.types import is_null

from .na import NA

# pylint: disable=invalid-name


def _as_type(x: Any, type_: Dtype, na: Any = None) -> Any:
    """Convert x or elements of x to certain type"""
    if is_scalar(x):
        if is_null(x) and na is not None:
            return na
        return type_(x) # type: ignore

    if hasattr(x, "astype"):
        if na is None:
            return x.astype(type_)

        na_mask = is_null(x)
        out = x.astype(type_)

        try:
            out[na_mask] = na
        except (ValueError, TypeError):
            out = out.astype(object)
            out[na_mask] = na

        return out

    return type(x)([_as_type(elem, type_=type_, na=na) for elem in x])


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
def as_integer(
    x: Any, integer_dtype: Dtype = numpy.int_, _keep_na: bool = True
) -> IntOrIter:
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
        _keep_na: If True, NAs will be kept, then the dtype will be object
            (interger_dtype ignored)

    Returns:
        Converted values according to the integer_dtype
    """
    if is_categorical(x):
        return categorized(x).codes
    return _as_type(x, integer_dtype, na=NA if _keep_na else None)


as_int = as_integer


@register_func(None, context=Context.EVAL)
def as_numeric(x: Any, _keep_na: bool = True) -> NumericOrIter:
    """Make elements numeric

    Args:
        x: The value to convert
        _keep_na: Whether to keep NAs as is. If True, will try to
            convert to double.

    Returns:
        Try `as_integer()` if failed then `as_float()`. If keep_na is True
    """
    if _keep_na:
        return as_double(x)
    try:
        return as_integer(x)
    except (ValueError, TypeError):
        return as_float(x)
