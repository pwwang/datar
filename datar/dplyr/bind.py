"""Bind multiple data frames by row and column

See https://github.com/tidyverse/dplyr/blob/master/R/bind.r
"""
from typing import Callable, Optional, Union

import pandas
from pandas import DataFrame, Categorical
from pandas.api.types import union_categoricals
from pipda import register_verb

from ..core.types import NoneType
from ..core.contexts import Context
from ..core.utils import copy_attrs, logger
from ..core.names import repair_names
from ..core.grouped import DataFrameGroupBy
from ..base.funcs import is_categorical
from ..tibble.funcs import tibble

@register_verb(
    (DataFrame, list, dict, NoneType),
    context=Context.EVAL
)
def bind_rows(
        _data: Optional[Union[DataFrame, list, dict]],
        *datas: Optional[Union[DataFrame, dict]],
        _id: Optional[str] = None,
        **kwargs: Union[DataFrame, dict]
) -> DataFrame:
    # pylint: disable=too-many-branches
    """Bind rows of give dataframes

    Args:
        _data: The seed dataframe to bind others
            Could be a dict or a list, keys/indexes will be used for _id col
        *datas: Other dataframes to combine
        _id: The name of the id columns
        **kwargs: A mapping of dataframe, keys will be used as _id col.

    Returns:
        The combined dataframe
    """
    if _id is not None and not isinstance(_id, str):
        raise ValueError("`_id` must be a scalar string.")

    def data_to_df(data):
        """Make a copy of dataframe or convert dict to a dataframe"""
        if isinstance(data, DataFrame):
            return data.copy()

        out = tibble(**data) # avoid varname error
        return out

    key_data = {}
    if isinstance(_data, list):
        for i, dat in enumerate(_data):
            if dat is not None:
                key_data[i] = data_to_df(dat)
    elif _data is not None:
        key_data[0] = data_to_df(_data)

    for i, dat in enumerate(datas):
        if dat is not None:
            key_data[len(key_data)] = data_to_df(dat)

    for key, val in kwargs.items():
        if val is not None:
            key_data[key] = data_to_df(val)

    if not key_data:
        return DataFrame()

    # handle categorical data
    for col in list(key_data.values())[0].columns:
        all_series = [
            dat[col] for dat in key_data.values()
            if col in dat and not dat[col].isna().all()
        ]
        all_categorical = [
            is_categorical(ser) or all(pandas.isna(ser)) for ser in all_series
        ]
        if all(all_categorical):
            union_cat = union_categoricals(all_series)
            for data in key_data.values():
                if col not in data: # in case it is 0-column df
                    continue
                data[col] = Categorical(
                    data[col],
                    categories=union_cat.categories,
                    ordered=is_categorical(data[col]) and data[col].cat.ordered
                )
        elif any(all_categorical):
            logger.warning("Factor information lost during rows binding.")

    if _id is not None:
        return pandas.concat(
            key_data.values(),
            keys=key_data.keys(),
            names=[_id, None]
        ).reset_index(level=0).reset_index(drop=True)

    return pandas.concat(key_data.values()).reset_index(drop=True)

@bind_rows.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *datas: Optional[Union[DataFrame, dict]],
        _id: Optional[str] = None,
        **kwargs: Union[DataFrame, dict]
) -> DataFrameGroupBy:

    data = bind_rows.dispatch(DataFrame)(_data, *datas, _id=_id, **kwargs)
    out = DataFrameGroupBy.construct_from(data, _data)
    copy_attrs(out, _data)
    return out

@register_verb((DataFrame, dict, NoneType), context=Context.EVAL)
def bind_cols(
        _data: Optional[Union[DataFrame, dict]],
        *datas: Optional[Union[DataFrame, dict]],
        _name_repair: Union[str, Callable] = "unique"
) -> DataFrame:
    """Bind columns of give dataframes

    Args:
        _data, *datas: Dataframes to combine

    Returns:
        The combined dataframe
    """
    if isinstance(_data, dict):
        _data = tibble(**_data)
    more_data = []
    for data in datas:
        if isinstance(data, dict):
            more_data.append(tibble(**data))
        else:
            more_data.append(data)
    if _data is not None:
        more_data.insert(0, _data)
    if not more_data:
        return DataFrame()
    ret = pandas.concat(more_data, axis=1)
    ret.columns = repair_names(ret.columns.tolist(), repair=_name_repair)
    return ret
