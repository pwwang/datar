"""Iterate over groups

https://github.com/tidyverse/dplyr/blob/master/R/group_split.R
https://github.com/tidyverse/dplyr/blob/master/R/group_map.R
https://github.com/tidyverse/dplyr/blob/master/R/group_trim.R
"""
from pipda import register_verb

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame

from ..core.contexts import Context
from ..core.tibble import TibbleGrouped, TibbleRowwise, reconstruct_tibble
from ..core.utils import logger, nargs
from ..base import is_factor, droplevels, setdiff, intersect
from .group_data import group_keys, group_rows, group_vars
from .group_by import group_by, ungroup
from .select import select
from .mutate import mutate
from .across import across
from .tidyselect import where


@register_verb(DataFrame, context=Context.EVAL)
def group_map(
    _data,
    _f,
    *args,
    _keep=False,
    **kwargs,
):
    """A generator to map function to data in each group"""
    keys = group_keys(_data, __ast_fallback="normal") if nargs(_f) > 1 else None
    for i, chunk in enumerate(
        group_split(_data, _keep=_keep, __ast_fallback="normal")
    ):
        if keys is None:
            yield _f(chunk)
        else:
            yield _f(chunk, keys.iloc[[i], :], *args, **kwargs)


def _group_map_list(_data, _f, *args, _keep=False, **kwargs):
    """List version of group_map"""
    return list(
        group_map(
            _data,
            _f,
            *args,
            **kwargs,
            _keep=_keep,
            __ast_fallback="normal",
        )
    )


group_map.list = register_verb(DataFrame, context=Context.PENDING)(
    _group_map_list
)


@register_verb(DataFrame, context=Context.EVAL)
def group_modify(_data, _f, *args, _keep=False, **kwargs):
    """Modify data in each group with func, returns a dataframe"""
    return _f(_data, *args, **kwargs)


@group_modify.register(TibbleGrouped)
def _(_data, _f, *args, _keep=False, **kwargs):
    gvars = group_vars(_data, __ast_fallback="normal")
    func = (lambda df, keys: _f(df)) if nargs(_f) == 1 else _f

    def fun(df, keys):
        res = func(df, keys, *args, **kwargs)
        if not isinstance(res, DataFrame):
            raise ValueError("The result of `_f` should be a data frame.")
        bad = intersect(res.columns, gvars, __ast_fallback="normal")
        if len(bad) > 0:
            raise ValueError(
                "The returned data frame cannot contain the original grouping "
                f"variables: {bad}."
            )

        return pd.concat(
            (
                keys.iloc[[0] * res.shape[0], :].reset_index(drop=True),
                res.reset_index(drop=True),
            ),
            axis=1,
        )

    chunks = group_map(_data, fun, _keep=_keep, __ast_fallback="normal")
    out = pd.concat(chunks, axis=0)

    return reconstruct_tibble(_data, out)


@register_verb(DataFrame, context=Context.EVAL)
def group_walk(_data, _f, *args, _keep=False, **kwargs):
    """Walk along data in each groups, but don't return anything"""
    # list to trigger generator
    list(
        group_map(
            _data,
            _f,
            *args,
            **kwargs,
            _keep=_keep,
            __ast_fallback="normal",
        )
    )


@register_verb(DataFrame)
def group_trim(_data, _drop=None):
    """Trim the unused group levels"""
    return _data


@group_trim.register(TibbleGrouped)
def _(_data, _drop=None):
    """Group trim on grouped data"""
    ungrouped = ungroup(_data, __ast_fallback="normal")

    fgroups = select(
        ungrouped,
        where(is_factor),
        __ast_fallback="normal",
    )
    dropped = mutate(
        ungrouped,
        across(
            fgroups.columns.tolist(),
            droplevels,
        ),
        __ast_fallback="normal",
    )

    return reconstruct_tibble(_data, dropped, drop=_drop)


@register_verb(DataFrame, context=Context.PENDING)
def with_groups(_data, _groups, _func, *args, **kwargs):
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
        grouped = ungroup(_data, __ast_fallback="normal")
    else:
        # all_columns = _data.columns
        # _groups = evaluate_expr(_groups, _data, Context.SELECT)
        # _groups = all_columns[vars_select(all_columns, _groups)]
        grouped = group_by(_data, _groups, __ast_fallback="normal")

    return _func(grouped, *args, **kwargs)


@register_verb(DataFrame, context=Context.EVAL)
def group_split(_data, *args, _keep=True, **kwargs):
    """Get a list of data in each group"""
    data = group_by(_data, *args, **kwargs, __ast_fallback="normal")
    yield from group_split_impl(data, _keep=_keep)


@group_split.register(TibbleGrouped, context=Context.EVAL)
def _(_data, *args, _keep=True, **kwargs):
    # data = group_by(_data, *args, **kwargs, _add=True)
    if args or kwargs:
        logger.warning(
            "`*args` and `**kwargs` are ignored in "
            "`group_split(<TibbleGrouped>)`, please use "
            "`group_by(..., _add=True) >> group_split()`."
        )
    return group_split_impl(_data, _keep=_keep)


@group_split.register(TibbleRowwise, context=Context.EVAL)
def _(_data, *args, _keep=None, **kwargs):
    if args or kwargs:
        logger.warning(
            "`*args` and `**kwargs` is ignored in "
            "`group_split(<TibbleRowwise>)`."
        )
    if _keep is not None:
        logger.warning(
            "`_keep` is ignored in " "`group_split(<TibbleRowwise>)`."
        )

    return group_split_impl(_data, _keep=True)


def _group_split_list(_data, *args, _keep=True, **kwargs):
    """List version of group_split"""
    return list(
        group_split(
            _data,
            *args,
            _keep=_keep,
            __ast_fallback="normal",
            **kwargs,
        )
    )


group_split.list = register_verb(DataFrame, context=Context.PENDING)(
    _group_split_list
)


def group_split_impl(data, _keep):
    """Implement splitting data frame by groups"""
    out = ungroup(data, __ast_fallback="normal")
    indices = group_rows(data, __ast_fallback="normal")

    if not _keep:
        remove = group_vars(data, __ast_fallback="normal")
        _keep = out.columns
        _keep = setdiff(_keep, remove, __ast_fallback="normal")
        out = out[_keep]

    for rows in indices:
        yield out.iloc[rows, :].reset_index(drop=True)
