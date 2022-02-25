"""Context dependent expressions

See souce https://github.com/tidyverse/dplyr/blob/master/R/context.R
"""
import numpy as np
from pandas import DataFrame, Series
from pipda import register_func

from ..core.tibble import Tibble, TibbleGrouped
from ..core.contexts import ContextBase
from ..core.middlewares import CurColumn
from ..core.utils import regcall
from ..base import setdiff


# n used directly in count
@register_func(DataFrame, verb_arg_only=True)
def n(_data: DataFrame, _context: ContextBase = None) -> int:
    """gives the current group size."""
    _data = _context.meta.get("input_data", _data)
    return _data.shape[0]


@n.register(TibbleGrouped)
def _(_data: TibbleGrouped, _context: ContextBase = None) -> Series:
    _data = _context.meta.get("input_data", _data)
    return _data._datar["grouped"].grouper.size()


@register_func(DataFrame, verb_arg_only=True)
def cur_data_all(_data: DataFrame, _context: ContextBase = None) -> DataFrame:
    """gives the current data for the current group
    (including grouping variables)"""
    return _context.meta.get("input_data", _data)


@register_func(DataFrame, verb_arg_only=True)
def cur_data(_data: DataFrame, _context: ContextBase = None) -> int:
    """gives the current data for the current group
    (excluding grouping variables)."""
    cols = regcall(setdiff, _data.columns, _data.group_vars or [])
    return _context.meta.get("input_data", _data)[cols]


@register_func(DataFrame, verb_arg_only=True)
def cur_group(_data: DataFrame, _context: ContextBase = None) -> Tibble:
    """gives the group keys, a tibble with one row and one column for
    each grouping variable."""
    _data = _context.meta.get("input_data", _data)
    out = _data.index.to_frame(index=False)

    if isinstance(_data, TibbleGrouped):
        return out.group_by(_data.group_vars)

    return out


@register_func(DataFrame, verb_arg_only=True)
def cur_group_id(_data: DataFrame, _context: ContextBase = None) -> int:
    """gives a unique numeric identifier for the current group."""
    return 0


@cur_group_id.register(TibbleGrouped)
def _(_data: TibbleGrouped, _context: ContextBase = None) -> Series:
    _data = _context.meta.get("input_data", _data)
    grouper = _data._datar["grouped"].grouper
    return Series(np.arange(grouper.ngroups), index=grouper.result_index)


@register_func(DataFrame, verb_arg_only=True)
def cur_group_rows(
    _data: DataFrame,
    _context: ContextBase = None,
) -> np.ndarray:
    """Gives the row indices for the current group.

    Args:
        _data: The dataFrame.

    Returns:
        The `_rows` from group data or row indexes
    """
    _data = _context.meta.get("input_data", _data)
    if not isinstance(_data, TibbleGrouped):
        return np.array([list(range(_data.shape[0]))], dtype=object)

    grouper = _data._datar["grouped"].grouper
    return np.array(
        list(grouper.indices[key])
        for key in grouper.group_keys_seq
    )


def cur_column() -> CurColumn:
    """Used in the functions of across. So we don't have to register it."""
    return CurColumn()
