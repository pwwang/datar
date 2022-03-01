"""Provides desc"""
import numpy as np
from pandas import Series
from pandas.api.types import is_scalar

from datar2.core.tibble import SeriesCategorical

from ..core.factory import func_factory


@func_factory("transform")
def desc(x):
    """Transform a vector into a format that will be sorted in descending order

    This is useful within arrange().

    The original API:
    https://dplyr.tidyverse.org/reference/desc.html

    Args:
        x: vector to transform

    Returns:
        The descending order of x
    """
    if is_scalar(x):
        return x

    return x[::-1]


desc.register(Series, lambda x: -x)


@desc.register(SeriesCategorical)
def _(x):
    cat = x.values
    code = cat.codes.astype(float)
    code[code == -1.0] = np.nan
    return Series(-code, name=x.name, index=x.index)
