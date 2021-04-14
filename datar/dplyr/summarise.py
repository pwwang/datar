"""Summarise each group to fewer rows"""

from typing import Any, Iterable, Mapping, Optional, Union
from pandas import DataFrame
from pipda import register_verb, evaluate_expr

from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.contexts import Context
from ..core.utils import (
    align_value, check_column_uniqueness, df_assign_item,
    name_mutatable_args, logger
)
from ..core.exceptions import ColumnNotExistingError
from ..core.grouped import DataFrameGroupBy
from .group_data import group_keys, group_vars, group_data
from .group_by import group_by_drop_default

@register_verb(DataFrame, context=Context.PENDING)
def summarise(
        _data: DataFrame,
        *args: Union[DataFrame, Mapping[str, Iterable[Any]]],
        _groups: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:
    """Summarise each group to fewer rows

    See https://dplyr.tidyverse.org/reference/summarise.html

    Args:
        _groups: Grouping structure of the result.
            - "drop_last": dropping the last level of grouping.
            - "drop": All levels of grouping are dropped.
            - "keep": Same grouping structure as _data.
            - "rowwise": Each row is its own group.
        *args, **kwargs: Name-value pairs, where value is the summarized
            data for each group

    Returns:
        The summary dataframe.
    """
    check_column_uniqueness(
        _data,
        "Can't transform a data frame with duplicate names."
    )
    return summarise_build(_data, *args, **kwargs)

@summarise.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *args: Union[DataFrame, Mapping[str, Iterable[Any]]],
        _groups: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:
    # empty
    sizes = []
    if group_data(_data).shape[0] == 0:
        out = summarise_build(_data, *args, **kwargs)
        sizes = []
    else:
        def apply_func(df):
            out = summarise(df, *args, **kwargs)
            sizes.append(out.shape[0])
            return out

        out = _data.group_apply(apply_func)

    if _groups is None:
        if all(size == 1 for size in sizes):
            _groups = "drop_last"
        else:
            _groups = "keep"

    g_keys = group_vars(_data)
    if _groups == "drop_last":
        if len(g_keys) > 1:
            if summarise.inform:
                logger.info(
                    '`summarise()` has grouped output by '
                    '%s (override with `_groups` argument)',
                    g_keys[:-1]
                )
            out = DataFrameGroupBy(
                out,
                _group_vars=g_keys[:-1],
                _drop=group_by_drop_default(_data)
            )
    elif _groups == "keep":
        if summarise.inform:
            logger.info(
                '`summarise()` has grouped output by '
                '%s (override with `_groups` argument)',
                g_keys
            )
        out = DataFrameGroupBy(
            out,
            _group_vars=g_keys,
            _drop=group_by_drop_default(_data)
        )
    # elif _group == "rowwise":
    #     ...
    # else: # drop
    return out

summarise.inform = True
summarize = summarise # pylint: disable=invalid-name

def summarise_build(
        _data: DataFrame,
        *args: Any,
        **kwargs: Any
) -> DataFrame:
    context = Context.EVAL.value
    named = name_mutatable_args(*args, **kwargs)

    out = group_keys(_data)
    for key, val in named.items():
        try:
            if out.shape[1] == 0:
                val = evaluate_expr(val, _data, context)
            else:
                val = evaluate_expr(val, out, context)
        except ColumnNotExistingError:
            # also recycle input
            val = evaluate_expr(val, _data, context)

        if val is None:
            continue

        if key.startswith(DEFAULT_COLUMN_PREFIX) and isinstance(val, DataFrame):
            # ignore key
            for name, ser in val.to_dict('series').items():
                ser = align_value(ser, out)
                df_assign_item(out, name, ser)
        elif isinstance(val, DataFrame):
            for name, ser in val.to_dict('series').items():
                ser = align_value(ser, out)
                df_assign_item(out, f'{key}${name}', ser)
        else:
            val = align_value(val, out)
            df_assign_item(out, key, val)

    return out
