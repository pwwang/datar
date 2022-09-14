"""Grouping metadata"""
from typing import List, Sequence, Union

from pipda import register_verb

from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.core.groupby import GroupBy

from ..core.tibble import Tibble, TibbleGrouped, TibbleRowwise
from ..core.utils import dict_get


@register_verb(DataFrame)
def group_data(_data: DataFrame) -> Tibble:
    """Returns a data frame that defines the grouping structure.

    Args:
        _data: The data frame

    Returns:
        The columns give the values of the grouping variables. The last column,
        always called `_rows`, is a list of integer vectors that gives the
        location of the rows in each group.

        Note that `_rows` are always 0-based.
    """
    return Tibble({"_rows": group_rows(_data, __ast_fallback="normal")})


@group_data.register((TibbleGrouped, GroupBy))
def _(_data: Union[TibbleGrouped, GroupBy]) -> Tibble:
    gpdata = group_keys(_data, __ast_fallback="normal")
    gpdata["_rows"] = group_rows(_data, __ast_fallback="normal")
    return gpdata


@register_verb(DataFrame)
def group_keys(_data: DataFrame) -> Tibble:
    """Just grouping data without the `_rows` columns

    Note:
        Additional arguments to select columns in dplyr are deprecated.
        Here we don't support it ahead.

    Args:
        _data: The data frame

    Returns:
        The group data without `_rows` column.
    """
    return Tibble(index=[0])


@group_keys.register(TibbleGrouped)
def _(_data: TibbleGrouped) -> Tibble:
    grouper = _data._datar["grouped"].grouper
    return Tibble(grouper.result_index.to_frame(index=False), copy=False)


# @group_keys.register(GroupBy)
# def _(_data: GroupBy) -> Tibble:
#     grouper = _data.grouper
#     return Tibble(grouper.result_index.to_frame(index=False), copy=False)


@group_keys.register(TibbleRowwise)
def _(_data: TibbleRowwise) -> Tibble:
    return Tibble(_data.loc[:, _data.group_vars])


@register_verb(DataFrame)
def group_rows(_data: DataFrame) -> List[List[int]]:
    """The locations of grouping structure, always 0-based."""
    rows = list(range(_data.shape[0]))
    return [rows]


@group_rows.register(TibbleGrouped)
def _(_data: TibbleGrouped) -> List[List[int]]:
    """Get row indices for each group"""
    return group_rows(_data._datar["grouped"], __ast_fallback="normal")


@group_rows.register(GroupBy)
def _(_data: GroupBy) -> List[List[int]]:
    """Get row indices for each group"""
    grouper = _data.grouper
    return [
        list(dict_get(grouper.indices, group_key))
        for group_key in grouper.result_index
    ]


@register_verb(DataFrame)
def group_indices(_data: DataFrame) -> List[int]:
    """Returns an integer vector the same length as `_data`.

    Always 0-based.
    """
    return [0] * _data.shape[0]


@group_indices.register(TibbleGrouped)
def _(_data: TibbleGrouped) -> List[int]:
    ret = {}
    for row in group_data(_data, __ast_fallback="normal").itertuples():
        for index in row[-1]:
            ret[index] = row.Index
    return [ret[key] for key in sorted(ret)]


@register_verb(DataFrame)
def group_vars(_data: DataFrame) -> Sequence[str]:
    """Gives names of grouping variables as character vector"""
    return getattr(_data, "group_vars", [])


# groups in dplyr returns R list
groups = group_vars
group_cols = group_vars


@register_verb(DataFrame)
def group_size(_data: DataFrame) -> Sequence[int]:
    """Gives the size of each group"""
    return [_data.shape[0]]


@group_size.register(TibbleGrouped)
def _(_data: TibbleGrouped) -> Sequence[int]:
    return list(map(len, group_rows(_data, __ast_fallback="normal")))


@register_verb(DataFrame)
def n_groups(_data: DataFrame) -> int:
    """Gives the total number of groups."""
    return 1


@n_groups.register(TibbleGrouped)
def _(_data: TibbleGrouped) -> int:
    return _data._datar["grouped"].ngroups


@n_groups.register(TibbleRowwise)
def _(_data: TibbleRowwise) -> int:
    return _data.shape[0]
