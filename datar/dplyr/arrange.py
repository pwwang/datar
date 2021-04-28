"""Arrange rows by column values

See source https://github.com/tidyverse/dplyr/blob/master/R/arrange.R
"""
from typing import Any
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import check_column_uniqueness
from ..core.grouped import DataFrameGroupBy
from ..base import union
from .group_data import group_vars
from .group_by import ungroup, group_by_drop_default
from .mutate import mutate

@register_verb(DataFrame, context=Context.PENDING)
def arrange(
        _data: DataFrame,
        *args: Any,
        _by_group: bool = False,
        **kwargs: Any
) -> DataFrame:
    """orders the rows of a data frame by the values of selected columns.

    The original API:
    https://dplyr.tidyverse.org/reference/arrange.html

    Args:
        _data: A data frame
        *series: Variables, or functions of variables.
            Use desc() to sort a variable in descending order.
        _by_group: If TRUE, will sort first by grouping variable.
            Applies to grouped data frames only.
        **kwargs: Name-value pairs that apply with mutate

    Returns:
        An object of the same type as _data.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    if not args and not kwargs:
        return _data

    check_column_uniqueness(
        _data,
        "Cannot arrange a data frame with duplicate names"
    )

    if not _by_group:
        sorting_df = mutate(ungroup(_data), *args, **kwargs, _keep="none")
        sorting_df = sorting_df.sort_values(by=sorting_df.columns.tolist())
    else:
        gvars = group_vars(_data)
        sorting_df = ungroup(mutate(_data, *args, **kwargs, _keep="none"))
        by = union(gvars, sorting_df.columns)
        sorting_df = sorting_df.sort_values(by=by)

    out = _data.loc[sorting_df.index, :].reset_index(drop=True)
    if isinstance(_data, DataFrameGroupBy):
        return _data.__class__(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )

    return out
