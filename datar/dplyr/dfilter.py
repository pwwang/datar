"""Subset rows using column values

See source https://github.com/tidyverse/dplyr/blob/master/R/filter.R
"""
from typing import Iterable

import numpy
from pandas import DataFrame, RangeIndex
from pipda import register_verb
from pipda.expression import Expression
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.types import is_scalar, is_null, BoolOrIter
from ..core.utils import copy_attrs, reconstruct_tibble, Array
from .group_data import group_data, group_vars

# pylint: disable=no-value-for-parameter


@register_verb(DataFrame, context=Context.EVAL)
def filter(  # pylint: disable=redefined-builtin
    _data: DataFrame,
    *conditions: Iterable[bool],
    _preserve: bool = False,
    _drop_index: bool = None,
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
    if _data.shape[0] == 0 or not conditions:
        return _data

    condition = None
    for cond in conditions:
        cond = _sanitize_condition(cond, _data.shape[0])
        condition = (
            cond
            if condition is None
            else numpy.logical_and(condition, cond)
        )

    out = _data.loc[condition, :]
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
    _drop_index: bool = None, # TODO?
) -> DataFrameGroupBy:
    """Filter on DataFrameGroupBy object"""
    if _data.shape[0] > 0:
        out = _data._datar_apply(
            filter,
            *conditions,
            _drop_index=False,
            __calling_env=CallingEnvs.REGULAR
        ).sort_index()
    else:
        out = _data.copy()

    out = reconstruct_tibble(_data, out)
    gdata = _filter_groups(out, _data)

    if not _preserve and _data.attrs["_group_drop"]:
        out.attrs['_group_data'] = gdata[gdata["_rows"].map(len) > 0]

    return out


@filter.register(DataFrameRowwise, context=Context.EVAL)
def _(
    _data: DataFrameRowwise,
    *conditions: Expression,
    _preserve: bool = False,
    _drop_index: bool = None,
) -> DataFrameGroupBy:
    """Filter on DataFrameGroupBy object"""
    out = filter.dispatch(DataFrame)(
        _data,
        *conditions,
        _preserve=_preserve,
        _drop_index=_drop_index
    )
    return reconstruct_tibble(_data, out, keep_rowwise=True)

def _filter_groups(new: DataFrameGroupBy, old: DataFrameGroupBy) -> DataFrame:
    """Filter non-existing rows in groupdata"""
    gdata = group_data(
        new,
        __calling_env=CallingEnvs.REGULAR
    ).set_index(group_vars(
        new,
        __calling_env=CallingEnvs.REGULAR
    ))["_rows"].to_dict()
    new_gdata = group_data(old, __calling_env=CallingEnvs.REGULAR).copy()
    for row in new_gdata.iterrows():
        ser = row[1]
        key = tuple(ser[:-1])
        if len(key) == 1:
            key = key[0]
        ser[-1] = gdata.get(key, [])
        new_gdata.loc[row[0], :] = ser

    new.attrs['_group_data'] = new_gdata
    return new_gdata

def _sanitize_condition(cond: BoolOrIter, length: int) -> numpy.ndarray:
    """Handle single condition"""
    if is_scalar(cond):
        out = Array([cond] * length)
    elif isinstance(cond, numpy.ndarray):
        out = cond
    else:
        out = Array(cond)
    out[is_null(out)] = False

    return out.astype(bool)
