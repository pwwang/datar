"""Iterate over groups

https://github.com/tidyverse/dplyr/blob/master/R/group_split.R
https://github.com/tidyverse/dplyr/blob/master/R/group_map.R
https://github.com/tidyverse/dplyr/blob/master/R/group_trim.R
"""
# TODO: add tests

from pipda.utils import evaluate_expr
from datar.core.types import StringOrIter
from typing import Any, Callable, Iterable, List, Optional

import pandas
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.utils import copy_attrs, logger, vars_select
from ..base import is_factor, droplevels, setdiff
from .group_data import group_rows, group_vars
from .group_by import group_by, group_by_drop_default, ungroup
from .select import select
from .mutate import mutate
from .across import across


@register_verb(DataFrame, context=Context.EVAL)
def group_map(
        _data: DataFrame,
        _f: Callable,
        *args: Any,
        _keep: bool = False,
        **kwargs: Any,
) -> List[Any]:
    """Map function to data in each group, returns a list"""
    for chunk in group_split(_data, _keep=_keep):
        yield _f(chunk, *args, **kwargs)

@register_verb(DataFrame, context=Context.EVAL)
def group_modify(
        _data: DataFrame,
        _f: Callable,
        *args: Any,
        _keep: bool = False,
        **kwargs: Any,
) -> DataFrame:
    """Modify data in each group with func, returns a dataframe"""
    out = group_map(_data, _f, _keep=_keep, *args, **kwargs)
    out = pandas.concat(out, ignore_index=True)
    if isinstance(_data, DataFrameGroupBy):
        out = _data.__class__(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )
    copy_attrs(out, _data)
    return out

@register_verb(DataFrame, context=Context.EVAL)
def group_walk(
        _data: DataFrame,
        _f: Callable,
        *args: Any,
        _keep: bool = False,
        **kwargs: Any,
) -> None:
    """Walk along data in each groups, but don't return anything"""
    group_map(_data, _f, _keep=_keep, *args, **kwargs)

@register_verb(DataFrame)
def group_trim(
        _data: DataFrame,
        _drop: Optional[bool] = None
) -> DataFrame:
    """Trim the unused group levels"""
    return _data

@group_trim.register(DataFrameGroupBy)
def _(
        _data: DataFrame,
        _drop: Optional[bool] = None
) -> DataFrameGroupBy:
    if _drop is None:
        _drop = group_by_drop_default(_drop)

    gvars = group_vars(_data)
    ungrouped = ungroup(_data)
    fgroups = select(ungrouped, across(gvars, is_factor))
    dropped = mutate(ungrouped, across(fgroups.columns.tolist(), droplevels))

    out = _data.__class__(
        dropped,
        _group_vars=gvars,
        _drop=_drop
    )
    copy_attrs(out, _data)
    return out

@register_verb(DataFrame, context=Context.PENDING)
def with_groups(
        _data: DataFrame,
        _groups: Optional[StringOrIter],
        _func: Callable,
        *args: Any,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Modify the grouping variables for a single operation.

    Args:
        _data: A data frame
        _groups: columns passed by group_by
            Use None to temporarily ungroup.
        _func: Function to apply to regrouped data.

    Returns:
        The new data frame with operations applied.
    """
    all_columns = _data.columns
    _groups = evaluate_expr(_groups, _data, Context.SELECT)
    _groups = all_columns[vars_select(all_columns, _groups)]
    grouped = group_by(_data, _group_vars=_groups)

    out = _func(grouped, *args, **kwargs)
    copy_attrs(out, _data)
    return out

@register_verb(DataFrame, context=Context.EVAL)
def group_split(
        _data: DataFrame,
        *args: Any,
        _keep: bool = True,
        **kwargs: Any
) -> Iterable[DataFrame]:
    """Get a list of data in each group"""
    data = group_by(_data, *args, **kwargs)
    return group_split_impl(data, _keep=_keep)

@group_split.register(DataFrameGroupBy, context=Context.EVAL)
def _(
        _data: DataFrame,
        *args: Any,
        _keep: bool = True,
        **kwargs: Any
) -> DataFrameGroupBy:
    data = group_by(_data, *args, **kwargs, _add=True)
    return group_split_impl(data, _keep=_keep)

@group_split.register(DataFrameRowwise, context=Context.EVAL)
def _(
        _data: DataFrame,
        *args: Any,
        _keep: Optional[bool] = None,
        **kwargs: Any
) -> DataFrameRowwise:
    if args or kwargs:
        logger.warning(
            "`*args` and `**kwargs` is ignored in "
            "group_split(<DataFrameRowwise>)."
        )
    if _keep is not None:
        logger.warning(
            "`_keep` is ignored in "
            "group_split(<DataFrameRowwise>)."
        )

    return group_split_impl(_data, _keep=True)


def group_split_impl(data: DataFrame, _keep: bool):
    out = ungroup(data)
    indices = group_rows(data)

    if not _keep:
        remove = group_vars(data)
        _keep = out.columns
        _keep = setdiff(_keep, remove)
        out = out[_keep]

    for rows in indices:
        yield out.iloc[rows, :]
