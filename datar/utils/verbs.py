"""Verbs ported from R-utils"""
from typing import Sequence, Union

import numpy
from pandas import DataFrame, Series

from pipda import register_verb


@register_verb((DataFrame, Series, list, tuple, numpy.ndarray))
def head(
    _data: Union[DataFrame, Series, Sequence, numpy.ndarray], n: int = 6
) -> DataFrame:
    """Get the first n rows of the dataframe or a vector

    Args:
        n: The number of rows/elements to return

    Returns:
        The dataframe with first n rows or a vector with first n elements
    """
    if isinstance(_data, DataFrame):
        return _data.head(n)
    return _data[:n]


@register_verb((DataFrame, Series, list, tuple, numpy.ndarray))
def tail(
    _data: Union[DataFrame, Series, Sequence, numpy.ndarray], n: int = 6
) -> DataFrame:
    """Get the last n rows of the dataframe or a vector

    Args:
        n: The number of rows/elements to return

    Returns:
        The dataframe with last n rows or a vector with last n elements
    """
    if isinstance(_data, DataFrame):
        return _data.tail(n)

    out = _data[-n:]
    try:
        return out.reset_index(drop=True) # type: ignore[union-attr]
    except AttributeError:
        return out
