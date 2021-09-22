"""Utilities for forcats"""
from pandas import Categorical
from ..core.types import ForcatsType, is_categorical, is_scalar


def check_factor(_f: ForcatsType) -> Categorical:
    """Make sure the input become a factor"""
    if not is_categorical(_f):
        if is_scalar(_f):
            _f = [_f]
        return Categorical(_f)

    return _f
