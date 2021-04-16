"""Set operations

https://github.com/tidyverse/dplyr/blob/master/R/sets.r
"""
from typing import Optional

import pandas
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.types import StringOrIter
from ..base.verbs import intersect, union, setdiff, setequal
from .bind import bind_rows

@intersect.register(DataFrame, context=Context.EVAL)
def _(
        _data: DataFrame,
        data2: DataFrame,
        *datas: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Intersect of two dataframes

    Args:
        _data, data2, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of intersect of input dataframes
    """
    from .distinct import distinct

    if not on:
        on = _data.columns.to_list()

    return distinct(pandas.merge(
        _data,
        data2,
        *datas,
        on=on,
        how='inner'
    ), *on)

@union.register(DataFrame, context=Context.EVAL)
def _(
        _data: DataFrame,
        data2: DataFrame,
        *datas: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Union of two dataframes

    Args:
        _data, data2, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of union of input dataframes
    """
    from .distinct import distinct
    if not on:
        on = _data.columns.to_list()

    return distinct(pandas.merge(
        _data,
        data2,
        *datas,
        on=on,
        how='outer'
    ), *on)

@setdiff.register(DataFrame, context=Context.EVAL)
def _(
        _data: DataFrame,
        data2: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Set diff of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of setdiff of input dataframes
    """
    from .distinct import distinct
    if not on:
        on = _data.columns.to_list()

    return distinct(_data.merge(
        data2,
        how='outer',
        on=on,
        indicator=True
    ).loc[
        lambda x: x['_merge'] == 'left_only'
    ].drop(columns=['_merge']), *on)

@register_verb(DataFrame, context=Context.EVAL)
def union_all(
        _data: DataFrame,
        data2: DataFrame
) -> DataFrame:
    """Union of all rows of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of union of all rows of input dataframes
    """
    return bind_rows(_data, data2)

@setequal.register(DataFrame, context=Context.EVAL)
def _(
        _data: DataFrame,
        data2: DataFrame
) -> bool:
    """Check if two dataframes equal

    Args:
        _data: The first dataframe
        data2: The second dataframe

    Returns:
        True if they equal else False
    """
    data1 = _data.sort_values(by=_data.columns.to_list()).reset_index(drop=True)
    data2 = data2.sort_values(by=data2.columns.to_list()).reset_index(drop=True)
    return data1.equals(data2)
