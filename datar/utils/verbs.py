"""Verbs ported from R-utils"""
from typing import Any
from pandas import DataFrame

from pipda import register_verb

from ..core.types import is_iterable

@register_verb
def head(_data: Any, n: int = 6) -> DataFrame:
    """Get the first n rows of the dataframe or a vector

    Args:
        n: The number of rows/elements to return

    Returns:
        The dataframe with first n rows or a vector with first n elements
    """
    if not is_iterable(_data):
        raise TypeError("`head` only works with iterable data.")
    if isinstance(_data, DataFrame):
        return _data.head(n)
    return _data[:n]

@register_verb
def tail(_data: Any, n: int = 6) -> DataFrame:
    """Get the last n rows of the dataframe or a vector

    Args:
        n: The number of rows/elements to return

    Returns:
        The dataframe with last n rows or a vector with last n elements
    """
    if not is_iterable(_data):
        raise TypeError("`tail` only works with iterable data.")
    if isinstance(_data, DataFrame):
        return _data.tail(n)
    return _data[-n:]
