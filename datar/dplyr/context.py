"""Context dependent expressions

See souce https://github.com/tidyverse/dplyr/blob/master/R/context.R
"""
import numpy as np
from pipda import register_verb

from ..core.backends.pandas import DataFrame, Series

from ..core.tibble import Tibble, TibbleGrouped
from ..core.middlewares import CurColumn
from ..core.utils import dict_get
from ..base import setdiff

from .group_data import group_data, group_keys


# n used directly in count
# @register_func(DataFrame, verb_arg_only=True)
@register_verb(DataFrame, dep=True)
def n(_data):
    """gives the current group size."""
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)
    return _data.shape[0]


@n.register(TibbleGrouped)
def _(_data):
    _data = _data._datar.get("summarise_source", _data)
    grouped = _data._datar["grouped"]

    out = grouped.grouper.size().to_frame().reset_index()
    out = out.groupby(
        grouped.grouper.names,
        sort=grouped.sort,
        observed=grouped.observed,
        dropna=grouped.dropna,
    )[0]

    return out


@register_verb(DataFrame, dep=True)
def cur_data_all(_data):
    """gives the current data for the current group
    (including grouping variables)"""
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)
    return Series([_data.copy()], dtype=object)


@cur_data_all.register(TibbleGrouped)
def _(_data):
    _data = _data._datar.get("summarise_source", _data)
    grouped = _data._datar["grouped"]
    return Series(
        [
            grouped.obj.loc[dict_get(grouped.grouper.groups, key), :]
            for key in grouped.grouper.result_index
        ],
        name="cur_data_all",
        dtype=object,
        index=grouped.grouper.result_index,
    )


@register_verb(DataFrame, dep=True)
def cur_data(_data):
    """gives the current data for the current group
    (excluding grouping variables)."""
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)
    cols = setdiff(
        _data.columns,
        _data.group_vars or [],
        __ast_fallback="normal",
    )
    return Series([_data[cols]], dtype=object)


@cur_data.register(TibbleGrouped)
def _(_data):
    _data = _data._datar.get("summarise_source", _data)
    cols = setdiff(
        _data.columns,
        _data.group_vars or [],
        __ast_fallback="normal",
    )
    return (
        _data._datar["grouped"].apply(lambda g: Series([g[cols]])).iloc[:, 0]
    )


@register_verb(DataFrame, dep=True)
def cur_group(_data):
    """gives the group keys, a tibble with one row and one column for
    each grouping variable."""
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)
    return Tibble(index=[0])


@cur_group.register(TibbleGrouped)
def _(_data):
    _data = _data._datar.get("summarise_source", _data)
    out = group_keys(_data, __ast_fallback="normal")
    # split each row as a df
    out = out.apply(lambda row: row.to_frame().T, axis=1)
    out.index = _data._datar["grouped"].grouper.result_index
    return out


@register_verb(DataFrame, dep=True)
def cur_group_id(_data):
    """gives a unique numeric identifier for the current group."""
    return 0


@cur_group_id.register(TibbleGrouped)
def _(_data):
    _data = _data._datar.get("summarise_source", _data)
    grouper = _data._datar["grouped"].grouper
    return Series(np.arange(grouper.ngroups), index=grouper.result_index)


@register_verb(DataFrame, dep=True)
def cur_group_rows(_data) -> np.ndarray:
    """Gives the row indices for the current group.

    Args:
        _data: The dataFrame.

    Returns:
        The `_rows` from group data or row indexes
    """
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)
    gdata = group_data(_data, __ast_fallback="normal")
    if isinstance(_data, TibbleGrouped):
        return gdata.set_index(_data.group_vars)["_rows"]

    return gdata["_rows"]


def cur_column() -> CurColumn:
    """Used in the functions of across. So we don't have to register it."""
    return CurColumn()
