"""Context dependent expressions

See souce https://github.com/tidyverse/dplyr/blob/master/R/context.R
"""
import numpy as np
from pandas import DataFrame, Series
from pipda import register_func

from ..core.tibble import Tibble, TibbleGrouped
from ..core.middlewares import CurColumn
from ..core.utils import regcall
from ..base import setdiff

from .group_data import group_data, group_keys


# n used directly in count
# @register_func(DataFrame, verb_arg_only=True)
@register_func(DataFrame)
def n(_data, _context=None):
    """gives the current group size."""
    _data = _context.meta.get("input_data", _data)
    return _data.shape[0]


@n.register(TibbleGrouped)
def _(_data, _context=None):
    _data = _context.meta.get("input_data", _data)
    return _data._datar["grouped"].grouper.size()


@register_func(DataFrame, verb_arg_only=True)
def cur_data_all(_data, _context=None):
    """gives the current data for the current group
    (including grouping variables)"""
    _data = _context.meta.get("input_data", _data)
    return Series([_data.copy()], dtype=object)


@cur_data_all.register(TibbleGrouped)
def _(_data, _context=None):
    _data = _context.meta.get("input_data", _data)
    grouped = _data._datar["grouped"]
    return Series(
        [
            grouped.obj.loc[grouped.grouper.groups[key], :]
            for key in grouped.grouper.result_index
        ],
        name="cur_data_all",
        dtype=object,
        index=grouped.grouper.result_index,
    )


@register_func(DataFrame, verb_arg_only=True)
def cur_data(_data, _context=None):
    """gives the current data for the current group
    (excluding grouping variables)."""
    _data = _context.meta.get("input_data", _data)
    cols = regcall(setdiff, _data.columns, _data.group_vars or [])
    return Series([_data[cols]], dtype=object)


@cur_data.register(TibbleGrouped)
def _(_data, _context=None):
    _data = _context.meta.get("input_data", _data)
    cols = regcall(setdiff, _data.columns, _data.group_vars or [])
    return (
        _data._datar["grouped"].apply(lambda g: Series([g[cols]])).iloc[:, 0]
    )


@register_func(DataFrame, verb_arg_only=True)
def cur_group(_data, _context=None):
    """gives the group keys, a tibble with one row and one column for
    each grouping variable."""
    _data = _context.meta.get("input_data", _data)
    return Tibble(index=[0])


@cur_group.register(TibbleGrouped)
def _(_data, _context=None):
    _data = _context.meta.get("input_data", _data)
    out = regcall(group_keys, _data)
    # split each row as a df
    out = out.apply(lambda row: row.to_frame().T, axis=1)
    out.index = _data._datar["grouped"].grouper.result_index
    return out


@register_func(DataFrame, verb_arg_only=True)
def cur_group_id(_data, _context=None):
    """gives a unique numeric identifier for the current group."""
    return 0


@cur_group_id.register(TibbleGrouped)
def _(_data, _context=None):
    _data = _context.meta.get("input_data", _data)
    grouper = _data._datar["grouped"].grouper
    return Series(np.arange(grouper.ngroups), index=grouper.result_index)


@register_func(DataFrame, verb_arg_only=True)
def cur_group_rows(
    _data,
    _context=None,
) -> np.ndarray:
    """Gives the row indices for the current group.

    Args:
        _data: The dataFrame.

    Returns:
        The `_rows` from group data or row indexes
    """
    _data = _context.meta.get("input_data", _data)
    gdata = regcall(group_data, _data)
    if isinstance(_data, TibbleGrouped):
        return gdata.set_index(_data.group_vars)["_rows"]

    return gdata["_rows"]


def cur_column() -> CurColumn:
    """Used in the functions of across. So we don't have to register it."""
    return CurColumn()
