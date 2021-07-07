"""Bind multiple data frames by row and column

See https://github.com/tidyverse/dplyr/blob/master/R/bind.r
"""
from typing import Callable, Union

import pandas
from pandas import DataFrame, Categorical
from pandas.api.types import union_categoricals
from pipda import register_verb

from ..core.types import NoneType, is_null, is_categorical
from ..core.contexts import Context
from ..core.utils import get_option, logger, reconstruct_tibble
from ..core.names import repair_names
from ..core.grouped import DataFrameGroupBy
from ..tibble import tibble


@register_verb((DataFrame, list, dict, NoneType), context=Context.EVAL)
def bind_rows(
    _data: Union[DataFrame, list, dict],
    *datas: Union[DataFrame, dict],
    _id: str = None,
    base0_: bool = None,
    _copy: bool = True,
    **kwargs: Union[DataFrame, dict],
) -> DataFrame:
    # pylint: disable=too-many-branches
    """Bind rows of give dataframes

    Original APIs https://dplyr.tidyverse.org/reference/bind.html

    Args:
        _data: The seed dataframe to bind others
            Could be a dict or a list, keys/indexes will be used for _id col
        *datas: Other dataframes to combine
        _id: The name of the id columns
        base0_: Whether `_id` starts from 0 or not, if no keys are provided.
            If `base0_` is not provided, will use
            `datar.base.get_option('index.base.0')`
        _copy: If `False`, do not copy data unnecessarily.
            Original API does not support this. This argument will be
            passed by to `pandas.concat()` as `copy` argument.
        **kwargs: A mapping of dataframe, keys will be used as _id col.

    Returns:
        The combined dataframe
    """
    base = int(not get_option("index.base.0", base0_))

    if _id is not None and not isinstance(_id, str):
        raise ValueError("`_id` must be a scalar string.")

    def data_to_df(data):
        """Make a copy of dataframe or convert dict to a dataframe"""
        if isinstance(data, DataFrame):
            return data.copy()

        out = tibble(**data)  # avoid varname error
        return out

    key_data = {}
    if isinstance(_data, list):
        for i, dat in enumerate(_data):
            if dat is not None:
                key_data[i + base] = data_to_df(dat)
    elif _data is not None:
        key_data[base] = data_to_df(_data)

    for i, dat in enumerate(datas):
        if isinstance(dat, list):
            for df in dat:
                key_data[len(key_data) + base] = data_to_df(df)
        elif dat is not None:
            key_data[len(key_data) + base] = data_to_df(dat)

    for key, val in kwargs.items():
        if val is not None:
            key_data[key] = data_to_df(val)

    if not key_data:
        return DataFrame()

    # handle categorical data
    for col in list(key_data.values())[0].columns:
        all_series = [
            dat[col]
            for dat in key_data.values()
            if col in dat and not dat[col].isna().all()
        ]
        all_categorical = [
            is_categorical(ser) or all(is_null(ser)) for ser in all_series
        ]
        if all(all_categorical):
            union_cat = union_categoricals(all_series)
            for data in key_data.values():
                if col not in data:  # in case it is 0-column df
                    continue
                data[col] = Categorical(
                    data[col],
                    categories=union_cat.categories,
                    ordered=is_categorical(data[col]) and data[col].cat.ordered,
                )
        elif any(all_categorical):
            logger.warning("Factor information lost during rows binding.")

    if _id is not None:
        return (
            pandas.concat(
                key_data.values(),
                keys=key_data.keys(),
                names=[_id, None],
                copy=_copy,
            )
            .reset_index(level=0)
            .reset_index(drop=True)
        )

    return pandas.concat(key_data.values(), copy=_copy).reset_index(drop=True)


@bind_rows.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy,
    *datas: Union[DataFrame, dict],
    _id: str = None,
    **kwargs: Union[DataFrame, dict],
) -> DataFrameGroupBy:

    data = bind_rows.dispatch(DataFrame)(_data, *datas, _id=_id, **kwargs)
    return reconstruct_tibble(_data, data)


@register_verb((DataFrame, dict, NoneType), context=Context.EVAL)
def bind_cols(
    _data: Union[DataFrame, dict],
    *datas: Union[DataFrame, dict],
    _name_repair: Union[str, Callable] = "unique",
    base0_: bool = None,
    _copy: bool = True,
) -> DataFrame:
    """Bind columns of give dataframes

    Note that unlike `dplyr`, mismatched dimensions are allowed and
    missing rows will be filled with `NA`s

    Args:
        _data: The seed dataframe to bind others
            Could be a dict, keys will be used for _id col
        *datas: other dataframes to bind
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        base0_: Whether the numeric suffix starts from 0 or not.
            If not specified, will use `datar.base.get_option('index.base.0')`.
        _copy: If `False`, do not copy data unnecessarily.
            Original API does not support this. This argument will be
            passed by to `pandas.concat()` as `copy` argument.

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
    ret = pandas.concat(more_data, axis=1, copy=_copy)
    ret.columns = repair_names(
        ret.columns.tolist(), repair=_name_repair, base0_=base0_
    )
    return ret
