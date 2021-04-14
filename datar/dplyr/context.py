"""Context dependent expressions

See souce https://github.com/tidyverse/dplyr/blob/master/R/context.R
"""
from typing import Any, Iterable

from pandas import DataFrame
from pipda import register_func

from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy

@register_func(context=Context.EVAL)
def n(series: Iterable[Any]) -> int:
    """gives the current group size."""
    return len(series)

@register_func(DataFrame)
def cur_data_all(_data: DataFrame) -> DataFrame:
    """gives the current data for the current group
    (including grouping variables)"""
    return _data
