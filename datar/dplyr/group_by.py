"""Group by verbs and functions
See source https://github.com/tidyverse/dplyr/blob/master/R/group-by.r
"""

from typing import Any, Union

from pipda import register_verb

from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.core.groupby import GroupBy

from ..core.exceptions import NameNonUniqueError
from ..core.tibble import Tibble, TibbleGrouped, TibbleRowwise
from ..core.contexts import Context
from ..core.utils import vars_select
from ..base import setdiff, union
from ..tibble import as_tibble

from .group_data import group_vars


@register_verb(DataFrame, context=Context.PENDING)
def group_by(
    _data: DataFrame,
    *args: Any,
    _add: bool = False,  # not working, since _data is not grouped
    _drop: bool = None,
    _sort: bool = False,
    _dropna: bool = False,
    **kwargs: Any,
) -> TibbleGrouped:
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
        _sort: Sort group keys.
        _dropna: If True, and if group keys contain NA values, NA values
            together with row/column will be dropped. If False, NA values
            will also be treated as the key in groups.
        *args: variables or computations to group by.
        **kwargs: Extra variables to group the dataframe

    Return:
        A `TibbleGrouped` object
    """
    from .mutate import mutate

    _data = mutate(_data, *args, __ast_fallback="normal", **kwargs)
    _data.reset_index(drop=True, inplace=True)

    if _drop is None:
        _drop = group_by_drop_default(_data)

    new_cols = _data._datar["mutated_cols"]
    if len(new_cols) == 0:
        return _data

    return _data.group_by(new_cols, drop=_drop, sort=_sort, dropna=_dropna)


@group_by.register(TibbleGrouped, context=Context.PENDING)
def _(
    _data: TibbleGrouped,
    *args: Any,
    _add: bool = False,
    _drop: bool = None,
    _sort: bool = False,
    _dropna: bool = False,
    **kwargs: Any,
) -> TibbleGrouped:
    """Group a grouped data frame"""
    from .mutate import mutate

    if _drop is None:
        _drop = group_by_drop_default(_data)

    _data = mutate(_data, *args, __ast_fallback="normal", **kwargs)
    new_cols = _data._datar["mutated_cols"]
    gvars = union(
        group_vars(_data, __ast_fallback="normal"),
        new_cols,
        __ast_fallback="normal",
    ) if _add else new_cols

    return group_by(
        Tibble(_data, copy=False),
        *gvars,
        _drop=_drop,
        _sort=_sort,
        _dropna=_dropna,
        __ast_fallback="normal",
    )


@register_verb(DataFrame, context=Context.SELECT)
def rowwise(
    _data: DataFrame,
    *cols: Union[str, int],
) -> TibbleRowwise:
    """Compute on a data frame a row-at-a-time

    See https://dplyr.tidyverse.org/reference/rowwise.html

    Args:
        _data: The dataframe
        *cols:  Variables to be preserved when calling summarise().
            This is typically a set of variables whose combination
            uniquely identify each row.

    Returns:
        A row-wise data frame
    """
    if not _data.columns.is_unique:
        raise NameNonUniqueError(
            "Cann't rowwise a data frame with duplicated names."
        )
    idxes = vars_select(_data.columns, *cols)
    gvars = _data.columns[idxes]
    return as_tibble(
        _data.reset_index(drop=True),
        __ast_fallback="normal",
    ).rowwise(gvars)


@rowwise.register(TibbleGrouped, context=Context.SELECT)
def _(
    _data: TibbleGrouped,
    *cols: Union[str, int],
) -> TibbleRowwise:
    # grouped dataframe's columns are unique already
    if cols:
        raise ValueError(
            "Can't re-group when creating rowwise data. "
            "Either first `ungroup()` or call `rowwise()` without arguments."
        )

    cols = _data.group_vars
    return rowwise(_data._datar["grouped"].obj, *cols, __ast_fallback="normal")


@rowwise.register(TibbleRowwise, context=Context.SELECT)
def _(_data: TibbleRowwise, *cols: Union[str, int]) -> TibbleRowwise:
    idxes = vars_select(_data.columns, *cols)
    gvars = _data.columns[idxes]
    return _data.rowwise(gvars)


@register_verb(object, context=Context.SELECT)
def ungroup(
    x: Any,
    *cols: Union[str, int],
) -> DataFrame:
    """Ungroup a grouped data

    See https://dplyr.tidyverse.org/reference/group_by.html

    Args:
        x: The data frame
        *cols: Variables to remove from the grouping variables.

    Returns:
        A data frame with selected columns removed from the grouping variables.
    """
    if cols:
        raise ValueError("`*cols` is not empty.")
    return x


@ungroup.register(TibbleGrouped, context=Context.SELECT)
def _(
    x: TibbleGrouped,
    *cols: Union[str, int],
) -> Union[Tibble, TibbleGrouped]:
    obj = x._datar["grouped"].obj
    if not cols:
        return Tibble(obj)

    old_groups = group_vars(x, __ast_fallback="normal")
    to_remove = vars_select(obj.columns, *cols)
    new_groups = setdiff(
        old_groups,
        obj.columns[to_remove],
        __ast_fallback="normal",
    )

    return group_by(obj, *new_groups, __ast_fallback="normal")


@ungroup.register(TibbleRowwise, context=Context.SELECT)
def _(
    x: TibbleRowwise,
    *cols: Union[str, int],
) -> DataFrame:
    if cols:
        raise ValueError("`*cols` is not empty.")
    return Tibble(x)


@ungroup.register(GroupBy, context=Context.SELECT)
def _(
    x: GroupBy,
    *cols: Union[str, int],
) -> DataFrame:
    if cols:
        raise ValueError("`*cols` is not empty.")
    return x.obj


def group_by_drop_default(_tbl: DataFrame) -> bool:
    """Get the groupby _drop attribute of dataframe"""
    grouped = getattr(_tbl, "_datar", {}).get("grouped", None)
    if not grouped:
        return True
    return grouped.observed
