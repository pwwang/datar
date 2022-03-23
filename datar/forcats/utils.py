"""Utilities for forcats"""
import numpy as np
from ..core.backends.pandas import Categorical, Series, Index
from ..core.backends.pandas.api.types import is_scalar, is_categorical_dtype
from ..core.backends.pandas.core.groupby import SeriesGroupBy


ForcatsRegType = (
    Series,
    SeriesGroupBy,
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
