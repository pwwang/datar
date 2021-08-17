"""Subset distinct/unique rows

See source https://github.com/tidyverse/dplyr/blob/master/R/distinct.R
"""
from typing import Any

import pandas
from pandas import DataFrame
from pipda import register_verb, register_func
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.utils import copy_attrs, reconstruct_tibble
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.types import is_scalar
from ..core.collections import Collection
from ..base import union, setdiff, intersect
from .mutate import mutate
from .group_by import ungroup


@register_verb(DataFrame, context=Context.PENDING)
def distinct(
    _data: DataFrame, *args: Any, _keep_all: bool = False, **kwargs: Any
) -> DataFrame:
    """Select only unique/distinct rows from a data frame.

    The original API:
    https://dplyr.tidyverse.org/reference/distinct.html

    Args:
        _data: The dataframe
        *args: and
        **kwargs: Optional variables to use when determining
            uniqueness.
        _keep_all: If TRUE, keep all variables in _data

    Returns:
        A dataframe without duplicated rows in _data
    """
    if not args and not kwargs:
        uniq = _data.drop_duplicates()
    else:
        # keep_none_prefers_new_order
        uniq = (
            mutate(
                _data,
                *args,
                **kwargs,
                _keep="none",
                __calling_env=CallingEnvs.REGULAR,
            )
        ).drop_duplicates()

    if not _keep_all:
        # keep original order
        out = uniq[
            union(
                intersect(
                    _data.columns,
                    uniq.columns,
                    __calling_env=CallingEnvs.REGULAR,
                ),
                setdiff(
                    uniq.columns,
                    _data.columns,
                    __calling_env=CallingEnvs.REGULAR,
                ),
                __calling_env=CallingEnvs.REGULAR,
            )
        ]
    else:
        out = _data.loc[uniq.index, :].copy()
        out[uniq.columns.tolist()] = uniq
    copy_attrs(out, _data)
    return out.reset_index(drop=True)


@distinct.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy, *args: Any, _keep_all: bool = False, **kwargs: Any
) -> DataFrameGroupBy:

    out = _data._datar_apply(
        lambda df: distinct(
            df,
            *args,
            **kwargs,
            _keep_all=_keep_all,
            __calling_env=CallingEnvs.REGULAR,
        )
    )

    return reconstruct_tibble(_data, out)


@distinct.register(DataFrameRowwise, context=Context.PENDING)
def _(
    _data: DataFrameRowwise, *args: Any, _keep_all: bool = False, **kwargs: Any
) -> DataFrameRowwise:
    # pylint: disable=no-value-for-parameter
    out = distinct.dispatch(DataFrame)(
        ungroup(_data, __calling_env=CallingEnvs.REGULAR),
        *args,
        **kwargs,
        _keep_all=_keep_all,
    )

    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_func(None, context=Context.EVAL)
def n_distinct(*data: Any, na_rm: bool = False) -> int:
    """Get the length of distince elements"""
    data = Collection(*(dat if is_scalar(dat) else list(dat) for dat in data))
    data = pandas.unique(data)
    if not na_rm:
        return len(data)
    return pandas.notna(data).sum()
