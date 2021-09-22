"""Iterate over groups

https://github.com/tidyverse/dplyr/blob/master/R/group_split.R
https://github.com/tidyverse/dplyr/blob/master/R/group_map.R
https://github.com/tidyverse/dplyr/blob/master/R/group_trim.R
"""

from typing import Any, Callable, Iterable, List

import pandas
from pandas import DataFrame
from pipda import register_verb, evaluate_expr
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.utils import (
    copy_attrs,
    logger,
    vars_select,
    nargs,
    reconstruct_tibble,
)
from ..core.types import StringOrIter
from ..base import is_factor, droplevels, setdiff, intersect
from .group_data import group_keys, group_rows, group_vars
from .group_by import group_by, ungroup
from .select import select
from .mutate import mutate
from .across import across
from .tidyselect import where


@register_verb(DataFrame, context=Context.EVAL)
def group_map(
    _data: DataFrame,
    _f: Callable,
    *args: Any,
    _keep: bool = False,
    **kwargs: Any,
) -> Iterable:
    """A generator to map function to data in each group"""
    keys = (
        group_keys(_data, __calling_env=CallingEnvs.REGULAR)
        if nargs(_f) > 1
        else None
    )
    for i, chunk in enumerate(
        group_split(_data, _keep=_keep, __calling_env=CallingEnvs.REGULAR)
    ):
        if keys is None:
            yield _f(chunk)
        else:
            yield _f(chunk, keys.iloc[[i], :], *args, **kwargs)


def _group_map_list(
    _data: DataFrame,
    _f: Callable,
    *args: Any,
    _keep: bool = False,
    **kwargs: Any,
) -> List:
    """List version of group_map"""
    return list(
        group_map(
            _data,
            _f,
            *args,
            _keep=_keep,
            **kwargs,
            __calling_env=CallingEnvs.REGULAR,
        )
    )


group_map.list = register_verb(DataFrame, context=Context.PENDING)(
    _group_map_list
)


@register_verb(DataFrame, context=Context.EVAL)
def group_modify(
    _data: DataFrame,
    _f: Callable,
    *args: Any,
    _keep: bool = False,
    **kwargs: Any,
) -> DataFrame:
    """Modify data in each group with func, returns a dataframe"""
    return _f(_data, *args, **kwargs)


@group_modify.register(DataFrameGroupBy)
def _(
    _data: DataFrameGroupBy,
    _f: Callable,
    *args: Any,
    _keep: bool = False,
    **kwargs: Any,
) -> DataFrameGroupBy:
    gvars = group_vars(_data, __calling_env=CallingEnvs.REGULAR)
    func = (lambda df, keys: _f(df)) if nargs(_f) == 1 else _f

    def fun(df, keys):
        res = func(df, keys, *args, **kwargs)
        if not isinstance(res, DataFrame):
            raise ValueError("The result of `_f` should be a data frame.")
        bad = intersect(res.columns, gvars, __calling_env=CallingEnvs.REGULAR)
        if bad:
            raise ValueError(
                "The returned data frame cannot contain the original grouping "
                f"variables: {bad}."
            )

        return pandas.concat(
            (
                keys.iloc[[0] * res.shape[0], :].reset_index(drop=True),
                res.reset_index(drop=True),
            ),
            axis=1,
        )

    chunks = group_map(
        _data, fun, _keep=_keep, __calling_env=CallingEnvs.REGULAR
    )
    out = pandas.concat(chunks, axis=0)

    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame, context=Context.EVAL)
def group_walk(
    _data: DataFrame,
    _f: Callable,
    *args: Any,
    _keep: bool = False,
    **kwargs: Any,
) -> None:
    """Walk along data in each groups, but don't return anything"""
    # list to trigger generator
    list(
        group_map(
            _data,
            _f,
            _keep=_keep,
            *args,
            **kwargs,
            __calling_env=CallingEnvs.REGULAR,
        )
    )


@register_verb(DataFrame)
def group_trim(_data: DataFrame, _drop: bool = None) -> DataFrame:
    """Trim the unused group levels"""
    return _data


@group_trim.register(DataFrameGroupBy)
def _(_data: DataFrame, _drop: bool = None) -> DataFrameGroupBy:
    """Group trim on grouped data"""
    ungrouped = ungroup(_data, __calling_env=CallingEnvs.REGULAR)

    fgroups = select(
        ungrouped, where(is_factor), __calling_env=CallingEnvs.REGULAR
    )
    dropped = mutate(
        ungrouped,
        across(fgroups.columns.tolist(), droplevels),
        __calling_env=CallingEnvs.REGULAR,
    )

    return reconstruct_tibble(_data, dropped, keep_rowwise=True)


@register_verb(DataFrame, context=Context.PENDING)
def with_groups(
    _data: DataFrame,
    _groups: StringOrIter,
    _func: Callable,
    *args: Any,
    **kwargs: Any,
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
    if _groups is None:
        grouped = ungroup(_data, __calling_env=CallingEnvs.REGULAR)
    else:
        all_columns = _data.columns
        _groups = evaluate_expr(_groups, _data, Context.SELECT)
        _groups = all_columns[vars_select(all_columns, _groups)]
        grouped = group_by(_data, *_groups, __calling_env=CallingEnvs.REGULAR)

    out = _func(grouped, *args, **kwargs)
    copy_attrs(out, _data)
    return out


@register_verb(DataFrame, context=Context.EVAL)
def group_split(
    _data: DataFrame, *args: Any, _keep: bool = True, **kwargs: Any
) -> Iterable[DataFrame]:
    """Get a list of data in each group"""
    data = group_by(_data, *args, **kwargs, __calling_env=CallingEnvs.REGULAR)
    yield from group_split_impl(data, _keep=_keep)


@group_split.register(DataFrameGroupBy, context=Context.EVAL)
def _(
    _data: DataFrame, *args: Any, _keep: bool = True, **kwargs: Any
) -> DataFrameGroupBy:
    # data = group_by(_data, *args, **kwargs, _add=True)
    if args or kwargs:
        logger.warning(
            "`*args` and `**kwargs` are ignored in "
            "`group_split(<DataFrameGroupBy)`, please use "
            "`group_by(..., _add=True) >> group_split()`."
        )
    return group_split_impl(_data, _keep=_keep)


@group_split.register(DataFrameRowwise, context=Context.EVAL)
def _(
    _data: DataFrame, *args: Any, _keep: bool = None, **kwargs: Any
) -> DataFrameRowwise:
    if args or kwargs:
        logger.warning(
            "`*args` and `**kwargs` is ignored in "
            "`group_split(<DataFrameRowwise>)`."
        )
    if _keep is not None:
        logger.warning(
            "`_keep` is ignored in " "`group_split(<DataFrameRowwise>)`."
        )

    return group_split_impl(_data, _keep=True)


def _group_split_list(
    _data: DataFrame, *args: Any, _keep: bool = True, **kwargs: Any
) -> Iterable[DataFrame]:
    """List version of group_split"""
    return list(
        group_split(
            _data,
            *args,
            _keep=_keep,
            **kwargs,
            __calling_env=CallingEnvs.REGULAR,
        )
    )


group_split.list = register_verb(DataFrame, context=Context.PENDING)(
    _group_split_list
)


def group_split_impl(data: DataFrame, _keep: bool):
    """Implement splitting data frame by groups"""
    out = ungroup(data, __calling_env=CallingEnvs.REGULAR)
    indices = group_rows(data, __calling_env=CallingEnvs.REGULAR)

    if not _keep:
        remove = group_vars(data, __calling_env=CallingEnvs.REGULAR)
        _keep = out.columns
        _keep = setdiff(_keep, remove, __calling_env=CallingEnvs.REGULAR)
        out = out[_keep]

    for rows in indices:
        yield out.iloc[rows, :]
