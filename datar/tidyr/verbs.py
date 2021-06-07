"""Verbs from R-tidyr"""
from functools import singledispatch
from typing import Any, Iterable, Optional, Union

import numpy
import pandas
from pandas import DataFrame
from pandas.core.groupby.generic import SeriesGroupBy
from pandas.core.series import Series
from pipda import register_verb

from ..core.utils import (
    copy_attrs, vars_select
)
from ..core.types import (
    DataFrameType, SeriesLikeType
)
from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy
from ..base import NA
from ..dplyr.group_by import group_by_drop_default
from ..dplyr.group_data import group_vars




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





@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def unite(
        _data: DataFrameType,
        col: str,
        *columns: str,
        sep: str = '_',
        remove: bool = True,
        na_rm: bool = False
) -> DataFrameType:
    """Unite multiple columns into one by pasting strings together

    Args:
        data: A data frame.
        col: The name of the new column, as a string or symbol.
        *columns: Columns to unite
        sep: Separator to use between values.
        remove: If True, remove input columns from output data frame.
        na_rm: If True, missing values will be remove prior to uniting
            each value.

    Returns:
        The dataframe with selected columns united
    """
    all_columns = _data.columns
    columns = all_columns[vars_select(all_columns, *columns)]

    out = _data.copy()

    def unite_cols(row):
        if na_rm:
            row = [elem for elem in row if elem is not NA]
        return sep.join(str(elem) for elem in row)

    out[col] = out[columns].agg(unite_cols, axis=1)
    if remove:
        out.drop(columns=columns, inplace=True)

    if isinstance(_data, DataFrameGroupBy):
        out = _data.__class__(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )
    copy_attrs(out, _data)
    return out
