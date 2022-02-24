"""Provides desc"""
from functools import singledispatch
from typing import Sequence

import numpy as np
from pandas import Series, Categorical
from pandas.core.groupby import SeriesGroupBy
from pipda import register_func

from ..core.contexts import Context


@singledispatch
def _desc(x) -> np.ndarray:
    return _desc(Series(x)).values


@_desc.register(Series)
def _(x: Series) -> Series:
    try:
        return -x
    except TypeError:
        cat = Categorical(x)
        code = cat.codes.astype(float)
        code[code == -1.0] = np.nan
        return Series(-code, name=x.name, index=x.index)


@_desc.register(SeriesGroupBy)
def _(x: SeriesGroupBy) -> Series:
    try:
        return -x.obj
    except TypeError:
        return x.apply(_desc)


@register_func(None, context=Context.EVAL)
def desc(x: Sequence) -> Sequence:
    """Transform a vector into a format that will be sorted in descending order

    This is useful within arrange().

    The original API:
    https://dplyr.tidyverse.org/reference/desc.html

    Args:
        x: vector to transform

    Returns:
        The descending order of x
    """
    return _desc(x)
