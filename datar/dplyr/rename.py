"""Rename columns

https://github.com/tidyverse/dplyr/blob/master/R/rename.R
"""
from typing import Any, Callable

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import vars_select
from ..core.grouped import DataFrameGroupBy
from .group_data import group_vars
from .select import _eval_select

@register_verb(DataFrame, context=Context.SELECT)
def rename(
        _data: DataFrame,
        **kwargs: str
) -> DataFrame:
    """Changes the names of individual variables using new_name = old_name
    syntax

    Args:
        _data: The dataframe
        **kwargs: The new_name = old_name pairs

    Returns:
        The dataframe with new names
    """
    gvars = group_vars(_data)
    all_columns = _data.columns
    selected, new_names = _eval_select(
        all_columns,
        _group_vars=gvars,
        **kwargs,
    )

    out = _data.copy()
    # new_names: old -> new
    # cannot do with duplicates
    # out.rename(columns=new_names, inplace=True)
    out.columns = [
        new_names.get(col, col)
        if i in selected else col
        for i, col in enumerate(all_columns)
    ]

    if isinstance(out, DataFrameGroupBy):
        out._group_vars = [new_names.get(name, name) for name in gvars]
        out._group_data.columns = out._group_vars + ['_rows']
    # attrs copied?
    return out

@register_verb(DataFrame, context=Context.SELECT)
def rename_with(
        _data: DataFrame,
        _fn: Callable[[str], str],
        *args: Any,
        **kwargs: Any
) -> DataFrame:
    """Renames columns using a function.

    Args:
        _data: The dataframe
        _fn: The function to rename a column
        *args: the columns to rename. If not specified, all columns are
            considered
        **kwargs: keyword arguments for `_fn`

    Returns:
        The dataframe with new names
    """
    if not args:
        cols = _data.columns.tolist()
    else:
        cols = args[0]
        args = args[1:]

    cols = _data.columns[vars_select(_data.columns, cols)]
    new_columns = {_fn(col, *args, **kwargs): col for col in cols}
    return rename(_data, **new_columns)
