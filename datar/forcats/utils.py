"""Utilities for forcats"""
import numpy as np
from pandas import Categorical, Series, Index
from pandas.api.types import is_scalar, is_categorical_dtype

ForcatsRegType = (
    Series,
    Categorical,
    Index,
    list,
    tuple,
    np.ndarray,
)


def check_factor(_f) -> Categorical:
    """Make sure the input become a factor"""
    if not is_categorical_dtype(_f):
        if is_scalar(_f):
            _f = [_f]
        return Categorical(_f)

    return _f
