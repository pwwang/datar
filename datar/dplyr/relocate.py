"""Relocate columns"""
from typing import Optional, Union

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy
from ..core.utils import list_diff, vars_select
from ..core.exceptions import ColumnNameInvalidError
from .group_by import group_by_drop_default

@register_verb(DataFrame, context=Context.SELECT)
def relocate(
        _data: DataFrame,
        column: str,
        *columns: str,
        _before: Optional[Union[int, str]] = None,
        _after: Optional[Union[int, str]] = None,
) -> DataFrame:
    """change column positions

    Args:
        _data: A data frame
        column: and
        *columns: Columns to move
        _before: and
        _after: Destination. Supplying neither will move columns to
            the left-hand side; specifying both is an error.

    Returns:
        An object of the same type as .data. The output has the following
        properties:
        - Rows are not affected.
        - The same columns appear in the output, but (usually) in a
            different place.
        - Data frame attributes are preserved.
        - Groups are not affected
    """
    groups = _data._group_vars if isinstance(_data, DataFrameGroupBy) else None
    all_columns = _data.columns.to_list()
    columns = vars_select(all_columns, column, *columns)
    rest_columns = list_diff(all_columns, columns)
    if _before is not None and _after is not None:
        raise ColumnNameInvalidError(
            'Only one of _before and _after can be specified.'
        )
    if _before is None and _after is None:
        rearranged = columns + rest_columns
    elif _before is not None:
        before_columns = vars_select(rest_columns, _before)
        cutpoint = min(rest_columns.index(bcol) for bcol in before_columns)
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    else: #_after
        after_columns = vars_select(rest_columns, _after)
        cutpoint = max(rest_columns.index(bcol) for bcol in after_columns) + 1
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    ret = _data[rearranged]
    if groups is not None:
        return DataFrameGroupBy(
            ret,
            _group_vars=groups,
            _drop=group_by_drop_default(_data)
        )
    return ret
