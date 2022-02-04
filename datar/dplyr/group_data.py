"""Grouping metadata"""

from typing import List
from pandas import DataFrame
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.grouped import DatarGroupBy, DatarRowwise
from ..core.utils import regcall


@register_verb(DataFrame)
def group_data(_data: DataFrame) -> DataFrame:
    """Returns a data frame that defines the grouping structure.

    Args:
        _data: The data frame

    Returns:
        The columns give the values of the grouping variables. The last column,
        always called `_rows`, is a list of integer vectors that gives the
        location of the rows in each group.

        Note that `_rows` are always 0-based.
    """
    return DataFrame({"_rows": regcall(group_rows, _data)})


@group_data.register(DatarGroupBy)
def _(_data: DatarGroupBy) -> DataFrame:
    grouper = _data.attrs["_grouped"].grouper
    gpdata = DataFrame(grouper.group_keys_seq, columns=grouper.names)
    gpdata["_rows"] = regcall(group_rows, _data)
    return gpdata


@group_data.register(DatarRowwise)
def _(_data: DatarRowwise) -> DataFrame:
    gvars = _data.attrs["_group_vars"]
    gpdata = _data.attrs["_grouped"].obj.loc[:, gvars]
    gpdata["_rows"] = regcall(group_rows, _data)
    return gpdata


@register_verb(DataFrame)
def group_keys(_data: DataFrame) -> DataFrame:
    """Just grouping data without the `_rows` columns

    Note:
        Additional arguments to select columns in dplyr are deprecated.
        Here we don't support it ahead.

    Args:
        _data: The data frame

    Returns:
        The group data without `_rows` column.
    """
    return DataFrame(index=[0])


@group_keys.register(DatarGroupBy)
def _(_data: DatarGroupBy) -> DataFrame:
    # .copy() allows future modifications
    return (
        group_data(_data, __calling_env=CallingEnvs.REGULAR)
        .iloc[:, :-1]
        .copy()
    )


@register_verb(DataFrame)
def group_rows(_data: DataFrame) -> List[List[int]]:
    """The locations of grouping structure, always 0-based."""
    rows = list(range(_data.shape[0]))
    return [rows]


@group_rows.register(DatarGroupBy)
def _(_data: DatarGroupBy) -> List[List[int]]:
    """Get row indices for each group"""
    grouper = _data.attrs["_grouped"].grouper
    return [
        list(grouper.groups[group_key]) for group_key in grouper.group_keys_seq
    ]


@register_verb(DataFrame)
def group_indices(_data: DataFrame) -> List[int]:
    """Returns an integer vector the same length as `_data`.

    Always 0-based.
    """
    return [0] * _data.shape[0]


@group_indices.register(DatarGroupBy)
def _(_data: DatarGroupBy) -> List[int]:
    ret = {}
    for row in group_data(
        _data, __calling_env=CallingEnvs.REGULAR
    ).itertuples():
        for index in row[-1]:
            ret[index] = row.Index
    return [ret[key] for key in sorted(ret)]


@register_verb(DataFrame)
def group_vars(_data: DataFrame) -> List[str]:
    """Gives names of grouping variables as character vector"""
    return _data.attrs.get("_group_vars", [])


# groups in dplyr returns R list
groups = group_vars
group_cols = group_vars


@register_verb(DataFrame)
def group_size(_data: DataFrame) -> List[int]:
    """Gives the size of each group"""
    return [_data.shape[0]]


@group_size.register(DatarGroupBy)
def _(_data: DatarGroupBy) -> List[int]:
    return list(
        map(len, group_data(_data, __calling_env=CallingEnvs.REGULAR)["_rows"])
    )


@register_verb(DataFrame)
def n_groups(_data: DataFrame) -> int:
    """Gives the total number of groups."""
    return 1


@n_groups.register(DatarGroupBy)
def _(_data: DatarGroupBy) -> int:
    return group_data(_data, __calling_env=CallingEnvs.REGULAR).shape[0]
