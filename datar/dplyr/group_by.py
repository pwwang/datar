"""Group by verbs and functions
See source https://github.com/tidyverse/dplyr/blob/master/R/group-by.r
"""

from typing import Any, Union

from pandas import DataFrame
from pipda import register_verb

from ..core.grouped import DatarGroupBy, DatarRowwise
from ..core.contexts import Context
from ..core.utils import (
    check_column_uniqueness,
    regcall,
    vars_select,
)
from ..base import setdiff, union

from .group_data import group_vars


@register_verb(DataFrame, context=Context.PENDING)
def group_by(
    _data: DataFrame,
    *args: Any,
    _add: bool = False,  # not working, since _data is not grouped
    _drop: bool = None,
    sort_: bool = False,
    dropna_: bool = False,
    **kwargs: Any,
) -> DatarGroupBy:
    """Takes an existing tbl and converts it into a grouped tbl where
    operations are performed "by group"

    See https://dplyr.tidyverse.org/reference/group_by.html and
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html

    Args:
        _data: The dataframe
        _add: When False, the default, `group_by()` will override
            existing groups. To add to the existing groups, use `_add=True`.
        _drop: Drop groups formed by factor levels that don't appear in the
            data? The default is True except when `_data` has been previously
            grouped with `_drop=False`.
        sort_: Sort group keys.
        dropna_: If True, and if group keys contain NA values, NA values
            together with row/column will be dropped. If False, NA values
            will also be treated as the key in groups.
        *args: variables or computations to group by.
        **kwargs: Extra variables to group the dataframe

    Return:
        A `DataFrameGroupBy` object
    """
    from .mutate import mutate

    _data = regcall(mutate, _data, *args, **kwargs)
    if _drop is None:
        _drop = group_by_drop_default(_data)
    new_cols = _data.attrs["_mutated_cols"]
    out = _data.groupby(new_cols, observed=_drop, sort=sort_, dropna=dropna_)
    return DatarGroupBy.from_grouped(out)


@group_by.register(DatarGroupBy, context=Context.PENDING)
def _(
    _data: DatarGroupBy,
    *args: Any,
    _add: bool = False,
    _drop: bool = None,
    sort_: bool = False,
    dropna_: bool = False,
    **kwargs: Any,
) -> DatarGroupBy:
    """Group a grouped data frame"""
    if not _add:
        return regcall(
            group_by,
            _data.obj,
            *args,
            _drop=_drop,
            sort_=sort_,
            dropna_=dropna_,
            **kwargs,
        )

    from .mutate import mutate

    _data = regcall(mutate, _data, *args, **kwargs)
    new_cols = _data.attrs["_mutated_cols"]
    gvars = regcall(
        union,
        regcall(group_vars, _data),
        new_cols,
    )
    out = _data.groupby(gvars, observed=_drop, sort=sort_)
    return DatarGroupBy.from_grouped(out)


@register_verb(DataFrame, context=Context.SELECT)
def rowwise(
    _data: DataFrame,
    *cols: Union[str, int],
    base0_: bool = None,
) -> DatarRowwise:
    """Compute on a data frame a row-at-a-time

    See https://dplyr.tidyverse.org/reference/rowwise.html

    Args:
        _data: The dataframe
        *cols:  Variables to be preserved when calling summarise().
            This is typically a set of variables whose combination
            uniquely identify each row.
        base0_: Whether indexes are 0-based if columns are selected by indexes.
            If not given, will use `datar.base.get_option('index.base.0')`

    Returns:
        A row-wise data frame
    """
    check_column_uniqueness(_data)
    idxes = vars_select(_data.columns, *cols, base0=base0_)
    group_vars = _data.columns[idxes].tolist()
    out = _data.groupby(list(range(_data.shape[0])), sort=False)
    return DatarRowwise.from_grouped(out, group_vars)


@rowwise.register(DatarGroupBy, context=Context.SELECT)
def _(
    _data: DatarGroupBy,
    *cols: str,
    base0_: bool = None,
) -> DatarRowwise:
    # grouped dataframe's columns are unique already
    if cols:
        raise ValueError(
            "Can't re-group when creating rowwise data. "
            "Either first `ungroup()` or call `rowwise()` without arguments."
        )

    return regcall(rowwise, _data.obj, *cols, base0_=base0_)


@rowwise.register(DatarRowwise, context=Context.SELECT)
def _(
    _data: DatarRowwise,
    *cols: str,
    base0_: bool = None,
) -> DatarRowwise:
    idxes = vars_select(_data.columns, *cols, base0=base0_)
    gvars = _data.columns[idxes].to_list()
    return DatarRowwise.from_groupby(_data, group_vars=gvars)


@register_verb(DataFrame, context=Context.SELECT)
def ungroup(
    x: DataFrame,
    *cols: Union[str, int],
    base0_: bool = None,
) -> DataFrame:
    """Ungroup a grouped data

    See https://dplyr.tidyverse.org/reference/group_by.html

    Args:
        x: The data frame
        *cols: Variables to remove from the grouping variables.
        base0_: If columns are selected with indexes, whether they are 0-based.
            If not given, will use `datar.base.get_option('index.base.0')`

    Returns:
        A data frame with selected columns removed from the grouping variables.
    """
    if cols:
        raise ValueError("`*cols` is not empty.")
    return x


@ungroup.register(DatarGroupBy, context=Context.SELECT)
def _(
    x: DatarGroupBy,
    *cols: Union[str, int],
    base0_: bool = None,
) -> Union[DataFrame, DatarGroupBy]:
    obj = x.attrs["_grouped"].obj
    if not cols:
        return obj

    old_groups = regcall(group_vars, x)
    to_remove = vars_select(obj.columns, *cols, base0=base0_)
    new_groups = regcall(
        setdiff,
        old_groups,
        obj.columns[to_remove],
    )

    return regcall(group_by, obj, *new_groups)


@ungroup.register(DatarRowwise, context=Context.SELECT)
def _(
    x: DatarRowwise,
    *cols: Union[str, int],
    base0_: bool = None,
) -> DataFrame:
    if cols:
        raise ValueError("`*cols` is not empty.")
    return x.attrs["_grouped"].obj


def group_by_drop_default(_tbl: DataFrame) -> bool:
    """Get the groupby _drop attribute of dataframe"""
    grouped = _tbl.attrs.get("_grouped", None)
    if not grouped:
        return True
    return grouped.observed
