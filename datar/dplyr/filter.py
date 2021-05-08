"""Subset rows using column values

See source https://github.com/tidyverse/dplyr/blob/master/R/filter.R
"""
from functools import singledispatch
from typing import Iterable, Optional

import numpy
from pandas import DataFrame, RangeIndex
from pipda import register_verb
from pipda.utils import Expression

from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.types import is_scalar
from ..core.utils import copy_attrs
from .group_by import group_by_drop_default
from .group_data import group_data, group_vars

@register_verb(DataFrame, context=Context.EVAL)
def filter( # pylint: disable=redefined-builtin
        _data: DataFrame,
        *conditions: Iterable[bool],
        _preserve: bool = False,
        _drop_index: Optional[bool] = None
) -> DataFrame:
    """Subset a data frame, retaining all rows that satisfy your conditions

    Args:
        *conditions: Expressions that return logical values
        _preserve: Relevant when the .data input is grouped.
            If _preserve = FALSE (the default), the grouping structure
            is recalculated based on the resulting data, otherwise
            the grouping is kept as is.
        _drop_index: Whether drop the index or not.
            When it is None and the index of _data is a RangeIndex, then
            the index is dropped.

    Returns:
        The subset dataframe
    """
    if _data.shape[0] == 0:
        return _data
    # check condition, conditions
    condition = numpy.array([True] * _data.shape[0])
    for cond in conditions:
        if is_scalar(cond):
            cond = numpy.array([cond] * _data.shape[0])
        condition = condition & cond
    try:
        condition = condition.values.flatten()
    except AttributeError:
        ...

    out = _data[condition]
    if _drop_index is None:
        _drop_index = isinstance(_data.index, RangeIndex)

    if _drop_index:
        out = out.reset_index(drop=True)
    copy_attrs(out, _data)
    return out

@filter.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *conditions: Expression,
        _preserve: bool = False,
        _drop_index: Optional[bool] = None # TODO ?
) -> DataFrameGroupBy:
    """Filter on DataFrameGroupBy object"""
    if _data.shape[0] > 0:
        out = _data.group_apply(
            lambda df: filter(df, *conditions, _drop_index=False),
            _drop_index=False
        ).sort_index()
    else:
        out = _data.copy()

    out = _data.__class__(
        out,
        _group_vars=group_vars(_data),
        _drop=group_by_drop_default(_data)
    )
    gdata = _filter_groups(out, _data)

    if not _preserve and _data.attrs.get('groupby_drop', True):
        out._group_data = gdata[gdata['_rows'].map(len) > 0]

    copy_attrs(out, _data)
    return out

@singledispatch
def _filter_groups(
        new: DataFrameGroupBy,
        old: DataFrameGroupBy
) -> DataFrame:
    """Filter non-existing rows in groupdata"""
    gdata = group_data(new).set_index(group_vars(new))['_rows'].to_dict()
    new_gdata = group_data(old).copy()
    for row in new_gdata.iterrows():
        ser = row[1]
        key = tuple(ser[:-1])
        if len(key) == 1:
            key = key[0]
        ser[-1] = gdata.get(key, [])
        new_gdata.loc[row[0], :] = ser

    new._group_data = new_gdata
    return new_gdata

@_filter_groups.register
def _(
        new: DataFrameRowwise,
        old: DataFrameRowwise # pylint: disable=unused-argument
) -> DataFrame:
    return new._group_data
