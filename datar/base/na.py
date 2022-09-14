"""NA related constants or functions"""
import numpy as np

from ..core.backends import pandas as pd
from ..core.defaults import NA_REPR
from ..core.factory import func_factory

from .arithmetic import SINGLE_ARG_SIGNATURE

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
    kind="transform",
    func=pd.isnull,
    qualname="datar.base.is_na",
    name="is_na",
    doc="""Test if a value is nullable or elements in the value is nullable

    Args:
        x: The value to test

    Returns:
        If `x` is scalar, returns a scalar bool. Otherwise, return an array
        of bools.
    """,
    signature=SINGLE_ARG_SIGNATURE,
)


@func_factory(kind="agg")
def any_na(x):
    """Check if any element in the value is nullable

    Args:
        x: The value to check
        recursive: Whether do a recursive check

    Returns:
        True if any element is nullable otherwise False
    """
    return pd.isnull(x).any()


is_infinite = func_factory(
    kind="transform",
    name="is_infinite",
    qualname="is_infinite",
    module="datar.base",
    doc="""Check if a value or values are infinite numbers

    Args:
        x: The value to check

    Returns:
        True if the value is infinite, False otherwise
        For iterable values, returns the element-wise results
    """,
    func=np.isinf,
    signature=SINGLE_ARG_SIGNATURE,
)

is_finite = func_factory(
    kind="transform",
    name="is_finite",
    qualname="is_finite",
    module="datar.base",
    doc="""Check if a value or values are finite numbers

    Args:
        x: The value to check

    Returns:
        True if the value is finite, False otherwise
        For iterable values, returns the element-wise results
    """,
    func=np.isfinite,
    signature=SINGLE_ARG_SIGNATURE,
)

is_nan = func_factory(
    kind="transform",
    name="is_nan",
    qualname="is_nan",
    module="datar.base",
    doc="""Check if a value or values are NaNs

    Args:
        x: The value to check

    Returns:
        True if the value is nan, False otherwise
        For iterable values, returns the element-wise results
    """,
    func=np.isnan,
    signature=SINGLE_ARG_SIGNATURE,
)
