"""Cast values between types"""
import numpy as np
from pipda import register_func

from ..core.backends import pandas as pd

from ..core.backends.pandas.api.types import is_scalar, is_categorical_dtype
from ..core.backends.pandas.core.groupby import SeriesGroupBy

from ..core.contexts import Context

from .factor import _ensure_categorical


def _as_type(x, type_, na=None):
    """Convert x or elements of x to certain type"""
    if is_scalar(x):
        if pd.isnull(x) and na is not None:
            return na
        return type_(x)  # type: ignore

    if isinstance(x, SeriesGroupBy):
        out = x.transform(_as_type, type_, na).groupby(
            x.grouper,
            observed=x.observed,
            sort=x.sort,
            dropna=x.dropna,
        )
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

    if hasattr(x, "astype"):
        if na is None:
            return x.astype(type_)

        na_mask = pd.isnull(x)
        out = x.astype(type_)

        try:
            out[na_mask] = na
        except (ValueError, TypeError):
            out = out.astype(object)
            out[na_mask] = na

        return out

    return type(x)([_as_type(elem, type_=type_, na=na) for elem in x])


@register_func(context=Context.EVAL)
def as_double(x):
    """Convert an object or elements of an iterable into double/float

    Args:
        x: The object

    Returns:
        Values converted into numpy.float64
    """
    return _as_type(x, np.double)


@register_func(context=Context.EVAL)
def as_float(x, float_dtype=np.float_):
    """Convert an object or elements of an iterable into double/float

    Args:
        x: The object

    Returns:
        Converted value according to the float dtype
    """
    return _as_type(x, float_dtype)


@register_func(context=Context.EVAL)
def as_integer(
    x,
    integer_dtype=np.int_,
    _keep_na=True,
):
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
    if isinstance(x, SeriesGroupBy) and is_categorical_dtype(x.obj):
        return x.obj.cat.codes.groupby(
            x.grouper,
            observed=x.observed,
            sort=x.sort,
            dropna=x.dropna,
        )

    if is_categorical_dtype(x):
        return _ensure_categorical(x).codes

    return _as_type(x, integer_dtype, na=np.nan if _keep_na else None)


as_int = as_integer


@register_func(context=Context.EVAL)
def as_numeric(x, _keep_na=True):
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
