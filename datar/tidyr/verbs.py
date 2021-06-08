"""Verbs from R-tidyr"""
from functools import singledispatch
from typing import Any, Iterable, Optional, Union

import numpy
import pandas
from pandas import DataFrame
from pandas.core.groupby.generic import SeriesGroupBy
from pandas.core.series import Series
from pipda import register_verb

from ..core.types import (
    SeriesLikeType
)
from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy




@singledispatch
def _replace_na(data: Iterable[Any], replace: Any) -> Iterable[Any]:
    """Replace NA for any iterables"""
    return type(data)(replace if pandas.isnull(elem) else elem for elem in data)

@_replace_na.register(numpy.ndarray)
@_replace_na.register(Series)
def _(data: SeriesLikeType, replace: Any) -> SeriesLikeType:
    """Replace NA for numpy.ndarray or Series"""
    ret = data.copy()
    ret[pandas.isnull(ret)] = replace
    return ret

@_replace_na.register(DataFrame)
def _(data: DataFrame, replace: Any) -> DataFrame:
    """Replace NA for numpy.ndarray or DataFrame"""
    return data.fillna(replace)

@_replace_na.register(DataFrameGroupBy)
@_replace_na.register(SeriesGroupBy)
def _(
        data: Union[DataFrameGroupBy, SeriesGroupBy],
        replace: Any
) -> Union[DataFrameGroupBy, SeriesGroupBy]:
    """Replace NA for grouped data, keep the group structure"""
    grouper = data.grouper
    ret = _replace_na(data, replace)
    return ret.groupby(grouper, dropna=False)

@register_verb(
    (DataFrame, DataFrameGroupBy, Series, numpy.ndarray, list, tuple, set),
    context=Context.EVAL
)
def replace_na(
        _data: Iterable[Any],
        data_or_replace: Optional[Any] = None,
        replace: Any = None
) -> Any:
    """Replace NA with a value

    This function can be also used not as a verb. As a function called as
    an argument in a verb, _data is passed implicitly. Then one could
    pass data_or_replace as the data to replace.

    Args:
        _data: The data piped in
        data_or_replace: When called as argument of a verb, this is the
            data to replace. Otherwise this is the replacement.
        replace: The value to replace with

    Returns:
        Corresponding data with NAs replaced
    """
    if data_or_replace is None and replace is None:
        return _data.copy()

    if replace is None:
        # no replace, then data_or_replace should be replace
        replace = data_or_replace
    else:
        # replace specified, determine data
        # If data_or_replace is specified, it's data
        _data = _data if data_or_replace is None else data_or_replace

    return _replace_na(_data, data_or_replace)
