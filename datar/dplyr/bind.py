"""Bind multiple data frames by row and column

See https://github.com/tidyverse/dplyr/blob/master/R/bind.r
"""
from pipda import register_verb

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame, Categorical
from ..core.backends.pandas.api.types import (
    union_categoricals,
    is_categorical_dtype,
    is_scalar,
)

from ..core.contexts import Context
from ..core.utils import logger
from ..core.names import repair_names
from ..core.tibble import Tibble, TibbleGrouped, reconstruct_tibble
from ..tibble import tibble


def _construct_tibble(data):
    if not isinstance(data, dict):
        return Tibble(data, copy=False)

    data = data.copy()
    for key, val in data.items():
        data[key] = [val] if is_scalar(val) else val

    return Tibble(data, copy=False)


@register_verb(
    (DataFrame, list, dict, type(None)),
    context=Context.EVAL,
)
def bind_rows(
    _data,
    *datas,
    _id=None,
    _copy=True,
    **kwargs,
):

    """Bind rows of give dataframes

    Original APIs https://dplyr.tidyverse.org/reference/bind.html

    Args:
        _data: The seed dataframe to bind others
            Could be a dict or a list, keys/indexes will be used for _id col
        *datas: Other dataframes to combine
        _id: The name of the id columns
        _copy: If `False`, do not copy data unnecessarily.
            Original API does not support this. This argument will be
            passed by to `pandas.concat()` as `copy` argument.
        **kwargs: A mapping of dataframe, keys will be used as _id col.

    Returns:
        The combined dataframe
    """

    if _id is not None and not isinstance(_id, str):
        raise ValueError("`_id` must be a scalar string.")

    key_data = {}
    if isinstance(_data, list):
        _data = [d for d in _data if d is not None]
        for i, dat in enumerate(_data):
            key_data[i] = _construct_tibble(dat)
    elif _data is not None:
        key_data[0] = _construct_tibble(_data)

    for i, dat in enumerate(datas):
        if isinstance(dat, list):
            for df in dat:
                key_data[len(key_data)] = _construct_tibble(df)
        elif dat is not None:
            key_data[len(key_data)] = _construct_tibble(dat)

    for key, val in kwargs.items():
        if val is not None:
            key_data[key] = _construct_tibble(val)

    if not key_data:
        return Tibble()

    # handle categorical data
    for col in list(key_data.values())[0].columns:
        all_series = [
            dat[col]
            for dat in key_data.values()
            if col in dat and not dat[col].isna().all()
        ]
        all_categorical = [
            is_categorical_dtype(ser) or pd.isnull(ser).all()
            for ser in all_series
        ]
        if all(all_categorical):
            union_cat = union_categoricals(all_series)
            for data in key_data.values():
                if col not in data:  # in case it is 0-column df
                    continue
                data[col] = Categorical(
                    data[col],
                    categories=union_cat.categories,
                    ordered=is_categorical_dtype(data[col])
                    and data[col].cat.ordered,
                )
        elif any(all_categorical):
            logger.warning("Factor information lost during rows binding.")

    if _id is not None:
        return (
            pd.concat(
                key_data.values(),
                keys=key_data.keys(),
                names=[_id, None],
                copy=_copy,
            )
            .reset_index(level=0)
            .reset_index(drop=True)
        )

    to_concat = [
        kdata
        for kdata in
        key_data.values()
        if kdata.shape[0] > 0
    ]
    if not to_concat:
        return key_data[0].loc[[], :]

    return pd.concat(to_concat, copy=_copy).reset_index(drop=True)


@bind_rows.register(TibbleGrouped, context=Context.PENDING)
def _(
    _data,
    *datas,
    _id=None,
    **kwargs,
):

    data = bind_rows.dispatch(DataFrame)(_data, *datas, _id=_id, **kwargs)
    return reconstruct_tibble(_data, data)


@register_verb(
    (DataFrame, dict, type(None)),
    context=Context.EVAL,
)
def bind_cols(
    _data,
    *datas,
    _name_repair="unique",
    _copy=True,
):
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
        return Tibble()

    ret = pd.concat(more_data, axis=1, copy=_copy)
    ret.columns = repair_names(ret.columns.tolist(), repair=_name_repair)
    return ret
