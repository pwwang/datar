"""NA related constants or functions"""
import numpy as np
import pandas as pd
from pandas.api.types import is_scalar
from pipda import register_func

from ..core.contexts import Context
from ..core.defaults import NA_REPR
from ..core.factory import func_factory

NA = np.nan
NaN = NA
Inf = np.inf

# Just for internal and testing uses
NA_character_ = NA_REPR
# Sentinels
NA_integer_ = np.random.randint(np.iinfo(np.int32).max)
NA_real_ = np.nan
NA_compex_ = complex(NA_real_, NA_real_)


is_na = func_factory(
    "transform",
    name="is_na",
    doc="""Test if a value is nullable or elements in the value is nullable

    Args:
        x: The value to test

    Returns:
        If `x` is scalar, returns a scalar bool. Otherwise, return an array
        of bools.
    """,
    func=pd.isnull,
)


@register_func(None, context=Context.EVAL)
def any_na(x, recursive=False):
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


is_infinite = func_factory(
    "transform",
    name="is_infinite",
    doc="""Check if a value or values are infinite numbers

    Args:
        x: The value to check

    Returns:
        True if the value is infinite, False otherwise
        For iterable values, returns the element-wise results
    """,
    func=np.isinf,
)

is_finite = func_factory(
    "transform",
    name="is_finite",
    doc="""Check if a value or values are finite numbers

    Args:
        x: The value to check

    Returns:
        True if the value is finite, False otherwise
        For iterable values, returns the element-wise results
    """,
    func=np.isfinite,
)

is_nan = func_factory(
    "transform",
    name="is_nan",
    doc="""Check if a value or values are NaNs

    Args:
        x: The value to check

    Returns:
        True if the value is nan, False otherwise
        For iterable values, returns the element-wise results
    """,
    func=np.isnan,
)
