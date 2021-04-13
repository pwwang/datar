
from typing import Any, Iterable
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import check_column_uniqueness
from ..core.grouped import DataFrameGroupBy
from .mutate import mutate

@register_verb(DataFrame, context=Context.EVAL)
def arrange(
        _data: DataFrame,
        *series: Iterable[Any],
        _by_group: bool = False
) -> DataFrame:
    """orders the rows of a data frame by the values of selected columns.

    The original API:
    https://dplyr.tidyverse.org/reference/arrange.html

    Args:
        _data: A data frame
        *series: Variables, or functions of variables.
            Use desc() to sort a variable in descending order.
        _by_group: If TRUE, will sort first by grouping variable.
            Applies to grouped data frames only.
        **kwargs: Name-value pairs that apply with mutate

    Returns:
        An object of the same type as _data.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    if not series:
        return _data

    check_column_uniqueness(
        _data,
        "Cannot arrange a data frame with duplicate names."
    )

    sorting_df = DataFrame(index=_data.index) >> mutate(*series)
    by = sorting_df.columns.tolist()
    sorting_df.sort_values(by=by, inplace=True)

    ret = _data.loc[sorting_df.index, :]
    return ret

@arrange.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *series: Any,
        _by_group: bool = False
) -> DataFrameGroupBy:
    """Arrange grouped dataframe"""
    if not _by_group:
        ret = _data.obj >> arrange(*series)
    else:
        ret = _data.obj >> arrange(
            *(_data.obj[col] for col in _data.grouper.names),
            *series
        )
    # copy_flags(ret, _data)
    # return group_df(ret, _data.grouper.names)
    return ret
