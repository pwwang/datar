"""Subset rows using column values

See source https://github.com/tidyverse/dplyr/blob/master/R/filter.R
"""
from typing import Iterable

import numpy
from pandas import DataFrame
from pipda import register_verb
from pipda.utils import Expression

from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy
from ..core.types import is_scalar
from ..core.utils import copy_attrs
from .group_by import group_by_drop_default
from .group_data import group_data, group_vars

@register_verb(DataFrame, context=Context.EVAL)
def filter(
        _data: DataFrame,
        *conditions: Iterable[bool],
        _preserve: bool = False,
        _drop_index: bool = True
) -> DataFrame:
    """Subset a data frame, retaining all rows that satisfy your conditions

    Args:
        condition, *conditions: Expressions that return logical values
        _preserve: Relevant when the .data input is grouped.
            If _preserve = FALSE (the default), the grouping structure
            is recalculated based on the resulting data, otherwise
            the grouping is kept as is.

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
    if _drop_index:
        out = out.reset_index(drop=True)
    copy_attrs(out, _data)
    return out

@filter.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *conditions: Expression,
        _preserve: bool = False
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

    if _preserve:
        gdata = group_data(out).set_index(group_vars(out))['_rows'].to_dict()
        new_gdata = group_data(_data).copy()
        for row in new_gdata.iterrows():
            ser = row[1]
            key = tuple(ser[:-1])
            ser[-1] = gdata[key] if key in gdata else []
            new_gdata.iloc[row[0], :] = ser

        out._group_data = new_gdata

    copy_attrs(out, _data)
    return out
