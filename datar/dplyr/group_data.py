"""Grouping metadata"""

from typing import List
from pandas import DataFrame
from pipda import register_verb

from ..core.grouped import DataFrameGroupBy
from ..base import setdiff

@register_verb(DataFrame)
def group_data(_data: DataFrame) -> DataFrame:
    """Returns a data frame that defines the grouping structure.

    Args:
        _data: The data frame

    Returns:
        The columns give the values of the grouping variables. The last column,
        always called `.rows`, is a list of integer vectors that gives the
        location of the rows in each group.
    """
    return DataFrame({
        '_rows': [list(range(_data.shape[0]))]
    })

@group_data.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy) -> DataFrame:
    return _data._group_data

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

@group_keys.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy) -> DataFrame:
    # .copy() allows future modifications
    return group_data(_data).iloc[:, :-1].copy()

@register_verb(DataFrame)
def group_rows(_data: DataFrame) -> List[List[int]]:
    """The locations of grouping structure"""
    return [list(range(_data.shape[0]))]

@group_rows.register(DataFrameGroupBy)
def _(_data: DataFrame) -> List[List[int]]:
    return group_data(_data)['_rows'].tolist()

@register_verb(DataFrame)
def group_indices(_data: DataFrame) -> List[int]:
    """Returns an integer vector the same length as `_data`"""
    return [0] * _data.shape[0]

@group_indices.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy) -> List[int]:
    ret = {}
    for row in group_data(_data).itertuples():
        for index in row[-1]:
            ret[index] = row.Index
    return [ret[key] for key in sorted(ret)]

@register_verb(DataFrame)
def group_vars(_data: DataFrame) -> List[str]:
    """Gives names of grouping variables as character vector"""
    # If it is a subdf of a DataFrameGroupBy, still be able to
    # return the group_vars
    index = _data.attrs.get('group_index', None)
    if index is None:
        return []
    gdata = _data.attrs['group_data']
    return setdiff(gdata.columns, ['_rows'])

@group_vars.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy) -> List[str]:
    return _data._group_vars

# groups in dplyr returns R list
groups = group_vars # pylint: disable=invalid-name
group_cols = group_vars # pylint: disable=invalid-name

@register_verb(DataFrame)
def group_size(_data: DataFrame) -> List[int]:
    """Gives the size of each group"""
    return [_data.shape[0]]

@group_size.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy) -> List[int]:
    return list(map(len, group_data(_data)['_rows']))

@register_verb(DataFrame)
def n_groups(_data: DataFrame) -> int:
    """Gives the total number of groups."""
    return 1

@n_groups.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy) -> int:
    return group_data(_data).shape[0]
