"""Group input by rows

See source https://github.com/tidyverse/dplyr/blob/master/R/rowwise.r
"""

from datar.dplyr.verbs import group_vars
from pandas import DataFrame
from pipda import register_verb

from ..core.utils import check_column_uniqueness, vars_select
from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise

@register_verb(DataFrame, context=Context.SELECT)
def rowwise(_data: DataFrame, *columns: str) -> DataFrame:
    """Compute on a data frame a row-at-a-time

    Args:
        _data: The dataframe
        *columns:  Variables to be preserved when calling summarise().
            This is typically a set of variables whose combination
            uniquely identify each row.

    Returns:
        A row-wise data frame
    """
    check_column_uniqueness(_data)
    columns = vars_select(_data.columns, columns)
    if not columns:
        return DataFrameRowwise(_data)
    return DataFrameRowwise(_data, _group_vars=_data.columns[columns])

@rowwise.register(DataFrameGroupBy, context=Context.SELECT)
def _(_data: DataFrameGroupBy, *columns: str) -> DataFrame:
    # grouped dataframe's columns are unique already
    if columns:
        raise ValueError(
            "Can't re-group when creating rowwise data. "
            "Either first `ungroup()` or call `rowwise()` without arguments."
        )
    return DataFrameRowwise(_data, _group_vars=group_vars(_data))
