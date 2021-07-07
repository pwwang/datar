"""Verbs ported from r-stats"""
from typing import Iterable

from pandas import DataFrame, isna
from pipda import register_verb

from ..core.defaults import NA_REPR
from ..core.grouped import DataFrameGroupBy

# pylint: disable=invalid-name
@register_verb(DataFrame)
def set_names(obj: DataFrame, names: Iterable[str]) -> DataFrame:
    """Set names of a dataframe"""
    names = [NA_REPR if isna(name) else name for name in names]
    obj = obj.copy()
    obj.columns = names
    return obj


@set_names.register(DataFrameGroupBy)
def _(obj: DataFrameGroupBy, names: Iterable[str]) -> DataFrameGroupBy:
    """Set names of a grouped/rowwise df"""
    obj = obj.copy()
    names_dict = dict(zip(obj.columns, names))
    obj.columns = names
    gvars = [names_dict[name] for name in obj._group_vars]
    obj._group_vars = gvars
    obj._group_data = obj._group_data.copy()
    obj._group_data.columns = gvars + [obj._group_data.columns[-1]]
    return obj
