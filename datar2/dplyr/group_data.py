"""Grouping metadata"""

from typing import List, Sequence, Union
from pandas import DataFrame
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.tibble import Tibble, TibbleGroupby, TibbleRowwise
from ..core.utils import regcall


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
    return Tibble({"_rows": regcall(group_rows, _data)})


@group_data.register((TibbleGroupby, TibbleRowwise))
def _(_data: Union[TibbleGroupby, TibbleRowwise]) -> Tibble:
    gpdata = regcall(group_keys, _data).copy()
    gpdata["_rows"] = regcall(group_rows, _data)
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


@group_keys.register(TibbleGroupby)
def _(_data: TibbleGroupby) -> Tibble:
    grouper = _data._datar_meta["grouped"].grouper
    return Tibble(grouper.group_keys_seq, columns=grouper.names)


@group_keys.register(TibbleRowwise)
def _(_data: TibbleRowwise) -> Tibble:
    return Tibble(_data.loc[:, _data.group_vars])


@register_verb(DataFrame)
def group_rows(_data: DataFrame) -> List[List[int]]:
    """The locations of grouping structure, always 0-based."""
    rows = list(range(_data.shape[0]))
    return [rows]


@group_rows.register(TibbleGroupby)
def _(_data: TibbleGroupby) -> List[List[int]]:
    """Get row indices for each group"""
    grouper = _data._datar_meta["grouped"].grouper
    return [
        list(grouper.groups[group_key])
        for group_key in grouper.group_keys_seq
    ]


@register_verb(DataFrame)
def group_indices(_data: DataFrame) -> List[int]:
    """Returns an integer vector the same length as `_data`.

    Always 0-based.
    """
    return [0] * _data.shape[0]


@group_indices.register(TibbleGroupby)
def _(_data: TibbleGroupby) -> List[int]:
    ret = {}
    for row in group_data(
        _data, __calling_env=CallingEnvs.REGULAR
    ).itertuples():
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


@group_size.register(TibbleGroupby)
def _(_data: TibbleGroupby) -> Sequence[int]:
    return list(map(len, regcall(group_rows, _data)))


@register_verb(DataFrame)
def n_groups(_data: DataFrame) -> int:
    """Gives the total number of groups."""
    return 1


@n_groups.register(TibbleGroupby)
def _(_data: TibbleGroupby) -> int:
    return _data._datar_meta["grouped"].grouper


@n_groups.register(TibbleRowwise)
def _(_data: TibbleRowwise) -> int:
    return _data.shape[0]
