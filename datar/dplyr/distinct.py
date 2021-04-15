"""Subset distinct/unique rows

See source https://github.com/tidyverse/dplyr/blob/master/R/distinct.R
"""
from typing import Any

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import copy_attrs
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..base.funcs import union, setdiff, intersect
from .mutate import mutate
from .group_by import group_by_drop_default, ungroup
from .group_data import group_vars

@register_verb(DataFrame, context=Context.PENDING)
def distinct(
        _data: DataFrame,
        *args: Any,
        _keep_all: bool = False,
        **kwargs: Any
) -> DataFrame:
    """Select only unique/distinct rows from a data frame.

    The original API:
    https://dplyr.tidyverse.org/reference/distinct.html

    Args:
        _data: The dataframe
        *args: and
        **kwargs: Optional variables to use when determining
            uniqueness.
        _keep_all: If TRUE, keep all variables in _data

    Returns:
        A dataframe without duplicated rows in _data
    """
    # keep_none_prefers_new_order
    uniq = mutate(_data, *args, **kwargs, _keep='none').drop_duplicates()
    if not _keep_all:
        # keep original order
        out = uniq[union(
            intersect(_data.columns, uniq.columns),
            setdiff(uniq.columns, _data.columns)
        )]
    else:
        out = _data.loc[uniq.index, :].copy()
        out[uniq.columns.tolist()] = uniq
    copy_attrs(out, _data)
    return out.reset_index(drop=True)

@distinct.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *args: Any,
        _keep_all: bool = False,
        **kwargs: Any
) -> DataFrameGroupBy:

    out = _data.group_apply(
        lambda df: distinct(df, *args, **kwargs, _keep_all=_keep_all)
    )
    return DataFrameGroupBy(
        out,
        _group_vars=group_vars(_data),
        _drop=group_by_drop_default(_data)
    )

@distinct.register(DataFrameRowwise, context=Context.PENDING)
def _(
        _data: DataFrameRowwise,
        *args: Any,
        _keep_all: bool = False,
        **kwargs: Any
) -> DataFrameRowwise:
    out = distinct.dispatch(DataFrame)(
        ungroup(_data),
        *args,
        **kwargs,
        _keep_all=_keep_all
    )
    return DataFrameRowwise(
        out,
        _group_vars=group_vars(_data),
        _drop=group_by_drop_default(_data)
    )
