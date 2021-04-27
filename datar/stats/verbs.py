"""Verbs ported from r-stats"""
from typing import Iterable

from pandas import DataFrame, isna
from pipda import register_verb

from ..core.defaults import NA_PLACEHOLDER
from ..dplyr.rename import rename

# pylint: disable=invalid-name
@register_verb(DataFrame)
def setNames(obj: DataFrame, names: Iterable[str]) -> DataFrame:
    """Set names of a dataframe"""
    names = [NA_PLACEHOLDER if isna(name) else name for name in names]
    return rename(obj, **dict(zip(names, obj.columns)))
