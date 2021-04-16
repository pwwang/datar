
"""Mutating joins"""
from typing import Iterable, Mapping, Optional, Union

import pandas
from pandas import DataFrame, Series
from pipda import register_verb

from ..core.contexts import Context
from ..core.types import StringOrIter
from ..core.grouped import DataFrameGroupBy
from ..base import intersect, setdiff
from .group_by import group_by_drop_default

def _join(
        x: DataFrame,
        y: DataFrame,
        how: str,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """General join"""
    if isinstance(by, dict):
        right_on = list(by.values())
        ret = pandas.merge(
            x, y,
            left_on=list(by.keys()),
            right_on=right_on,
            how=how,
            copy=copy,
            suffixes=suffix
        )
        if not keep:
            ret.drop(columns=right_on, inplace=True)
    else:
        ret = pandas.merge(
            x, y,
            on=by,
            how=how,
            copy=copy,
            suffixes=suffix
        )

    if isinstance(x, DataFrameGroupBy):
        return DataFrameGroupBy(
            ret,
            _group_vars=x._group_vars,
            _drop=group_by_drop_default(x)
        )

    return ret


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def inner_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in x and y.

    Args:
        x, y: A pair of data frames
        by: A character vector of variables to join by.
        copy: If x and y are not from the same data source, and copy is
            TRUE, then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a
            potentially expensive operation so you must opt into it.
        suffix: If there are non-joined duplicate variables in x and y,
            these suffixes will be added to the output to disambiguate them.
            Should be a character vector of length 2.
        keep: Should the join keys from both x and y be preserved in the output?

    Returns:
        The joined dataframe
    """
    return _join(
        x, y,
        how='inner',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def left_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in x.

    See inner_join()
    """
    return _join(
        x, y,
        how='left',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def right_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in y.

    See inner_join()
    """
    return _join(
        x, y,
        how='right',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def full_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in x or y.

    See inner_join()
    """
    return _join(
        x, y,
        how='outer',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def semi_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False
) -> DataFrame:
    """Returns all rows from x with a match in y.

    See inner_join()
    """
    ret = pandas.merge(
        x, y,
        on=by,
        how='left',
        copy=copy,
        suffixes=['', '_y'],
        indicator=True
    )
    ret = ret.loc[ret._merge == 'both', x.columns.tolist()]

    if isinstance(x, DataFrameGroupBy):
        return DataFrameGroupBy(
            ret,
            _group_vars=x._group_vars,
            _drop=group_by_drop_default(x)
        )

    return ret

@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def anti_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False
) -> DataFrame:
    """Returns all rows from x without a match in y.

    See inner_join()
    """
    ret = pandas.merge(
        x, y,
        on=by,
        how='left',
        copy=copy,
        suffixes=['', '_y'],
        indicator=True
    )
    ret = ret.loc[ret._merge != 'both', x.columns.tolist()]

    if isinstance(x, DataFrameGroupBy):
        return DataFrameGroupBy(
            ret,
            _group_vars=x._group_vars,
            _drop=group_by_drop_default(x)
        )

    return ret

@register_verb(DataFrame, context=Context.EVAL)
def nest_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """Returns all rows and columns in x with a new nested-df column that
    contains all matches from y

    See inner_join()
    """
    on = by
    if isinstance(by, (list, tuple, set)):
        on = dict(zip(by, by))
    elif by is None:
        common_cols = intersect(x.columns.tolist(), y.columns)
        on = dict(zip(common_cols, common_cols))
    elif not isinstance(by, dict):
        on = {by: by}

    if copy:
        x = x.copy()

    def get_nested_df(row: Series) -> DataFrame:
        condition = None
        for key in on:
            if condition is None:
                condition = y[on[key]] == row[key]
            else:
                condition = condition and (y[on[key]] == row[key])
        df = filter(y, condition)
        if not keep:
            df = df[setdiff(df.columns, on.values())]
        if suffix:
            for col in df.columns:
                if col in x:
                    x.rename(columns={col: f'{col}{suffix[0]}'}, inplace=True)
                    df.rename(columns={col: f'{col}{suffix[1]}'}, inplace=True)
        return df

    y_matched = x.apply(get_nested_df, axis=1)
    y_name = getattr(y, '__dfname__', None)
    if y_name:
        y_matched = y_matched.to_frame(name=y_name)
    return pandas.concat([x, y_matched], axis=1)
