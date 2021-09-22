"""Context dependent expressions

See souce https://github.com/tidyverse/dplyr/blob/master/R/context.R
"""
from typing import Any, Iterable, List

from pandas import DataFrame
from pipda import register_func
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.middlewares import CurColumn
from ..base import setdiff
from .group_data import group_vars


# n used directly in count
@register_func(context=Context.EVAL, summarise_prefers_input=True)
def n(series: Iterable[Any]) -> int:
    """gives the current group size."""
    return len(series)


@register_func(DataFrame, verb_arg_only=True, summarise_prefers_input=True)
def cur_data_all(_data: DataFrame) -> DataFrame:
    """gives the current data for the current group
    (including grouping variables)"""
    return _data


@register_func(DataFrame, verb_arg_only=True, summarise_prefers_input=True)
def cur_data(_data: DataFrame) -> int:
    """gives the current data for the current group
    (excluding grouping variables)."""
    return _data[
        setdiff(
            _data.columns,
            group_vars(_data, __calling_env=CallingEnvs.REGULAR),
            __calling_env=CallingEnvs.REGULAR,
        )
    ]


@register_func(DataFrame, verb_arg_only=True, summarise_prefers_input=True)
def cur_group(_data: DataFrame) -> DataFrame:
    """gives the group keys, a tibble with one row and one column for
    each grouping variable."""
    index = _data.attrs.get("_group_index", None)
    if index is None:
        return DataFrame(index=range(_data.shape[0]))

    gdata = _data.attrs["_group_data"]
    return gdata.iloc[[index], :-1]


@register_func(DataFrame, verb_arg_only=True, summarise_prefers_input=True)
def cur_group_id(_data: DataFrame) -> int:
    """gives a unique numeric identifier for the current group."""
    return _data.attrs.get("_group_index", 1)


@register_func(DataFrame, verb_arg_only=True, summarise_prefers_input=True)
def cur_group_rows(_data: DataFrame) -> List[int]:
    """Gives the row indices for the current group.

    Args:
        _data: The dataFrame.

    Returns:
        The `_rows` from group data or row indexes (always 0-based).
    """
    index = _data.attrs.get("_group_index", None)
    if index is None:
        return list(range(_data.shape[0]))
    return _data.attrs["_group_data"].loc[index, "_rows"]


def cur_column() -> CurColumn:
    """Used in the functions of across. So we don't have to register it."""
    return CurColumn()
