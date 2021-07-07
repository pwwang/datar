"""Mutating joins"""
from typing import Iterable, Mapping, Union

import pandas
from pandas import DataFrame, Series, Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from pipda import register_verb

from ..core.contexts import Context
from ..core.types import StringOrIter, is_scalar
from ..core.grouped import DataFrameGroupBy
from ..core.utils import reconstruct_tibble
from ..base import intersect, setdiff, union
from .group_by import group_by_drop_default
from .group_data import group_data, group_vars
from .dfilter import filter as filter_


def _join(
    x: DataFrame,
    y: DataFrame,
    how: str,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    # na_matches: str = "", # TODO: how?
    keep: bool = False,
) -> DataFrame:
    """General join"""
    if by is not None and not by:
        ret = pandas.merge(x, y, how="cross", copy=copy, suffixes=suffix)
    elif isinstance(by, dict):
        right_on = list(by.values())
        ret = pandas.merge(
            x,
            y,
            left_on=list(by.keys()),
            right_on=right_on,
            how=how,
            copy=copy,
            suffixes=suffix,
        )
        if not keep:
            ret.drop(columns=right_on, inplace=True)
    elif keep:
        if by is None:
            by = intersect(x.columns, y.columns)
        # on=... doesn't keep both by columns in left and right
        left_on = [f"{col}{suffix[0]}" for col in by]
        right_on = [f"{col}{suffix[1]}" for col in by]
        x = x.rename(columns=dict(zip(by, left_on)))
        y = y.rename(columns=dict(zip(by, right_on)))
        ret = pandas.merge(
            x,
            y,
            left_on=left_on,
            right_on=right_on,
            how=how,
            copy=copy,
            suffixes=suffix,
        )
    else:
        if by is None:
            by = intersect(x.columns, y.columns)
        by = [by] if is_scalar(by) else by  # type: ignore
        ret = pandas.merge(x, y, on=by, how=how, copy=copy, suffixes=suffix)
        for col in by:
            if is_categorical_dtype(x[col]) and is_categorical_dtype(y[col]):
                ret[col] = Categorical(
                    ret[col],
                    categories=union(
                        x[col].cat.categories, y[col].cat.categories
                    ),
                )

    return reconstruct_tibble(x, ret, keep_rowwise=True)


@register_verb(
    DataFrame, context=Context.EVAL, extra_contexts={"by": Context.SELECT}
)
def inner_join(
    x: DataFrame,
    y: DataFrame,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    keep: bool = False,
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
    return _join(x, y, how="inner", by=by, copy=copy, suffix=suffix, keep=keep)


@register_verb(
    DataFrame, context=Context.EVAL, extra_contexts={"by": Context.SELECT}
)
def left_join(
    x: DataFrame,
    y: DataFrame,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    keep: bool = False,
) -> DataFrame:
    """Mutating joins including all rows in x.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    return _join(x, y, how="left", by=by, copy=copy, suffix=suffix, keep=keep)


@register_verb(
    DataFrame, context=Context.EVAL, extra_contexts={"by": Context.SELECT}
)
def right_join(
    x: DataFrame,
    y: DataFrame,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    keep: bool = False,
) -> DataFrame:
    """Mutating joins including all rows in y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)

    Note:
        The rows of the order is preserved according to `y`. But `dplyr`'s
        `right_join` preserves order from `x`.
    """
    return _join(x, y, how="right", by=by, copy=copy, suffix=suffix, keep=keep)


@register_verb(
    DataFrame, context=Context.EVAL, extra_contexts={"by": Context.SELECT}
)
def full_join(
    x: DataFrame,
    y: DataFrame,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    keep: bool = False,
) -> DataFrame:
    """Mutating joins including all rows in x or y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    return _join(x, y, how="outer", by=by, copy=copy, suffix=suffix, keep=keep)


@register_verb(
    DataFrame, context=Context.EVAL, extra_contexts={"by": Context.SELECT}
)
def semi_join(
    x: DataFrame,
    y: DataFrame,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
) -> DataFrame:
    """Returns all rows from x with a match in y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    ret = pandas.merge(
        x,
        y,
        on=by,
        how="left",
        copy=copy,
        suffixes=["", "_y"],
        indicator="__merge__",
    )
    ret = ret.loc[ret["__merge__"] == "both", x.columns.tolist()]

    return reconstruct_tibble(x, ret)


@register_verb(
    DataFrame, context=Context.EVAL, extra_contexts={"by": Context.SELECT}
)
def anti_join(
    x: DataFrame,
    y: DataFrame,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
) -> DataFrame:
    """Returns all rows from x without a match in y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    ret = pandas.merge(
        x, y, on=by, how="left", copy=copy, suffixes=["", "_y"], indicator=True
    )
    ret = ret.loc[ret._merge != "both", x.columns.tolist()]

    return reconstruct_tibble(x, ret)


@register_verb(
    DataFrame, context=Context.EVAL, extra_contexts={"by": Context.SELECT}
)
def nest_join(
    x: DataFrame,
    y: DataFrame,
    by: Union[StringOrIter, Mapping[str, str]] = None,
    copy: bool = False,
    keep: bool = False,
    name: str = None,
) -> DataFrame:
    """Returns all rows and columns in x with a new nested-df column that
    contains all matches from y

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    on = by
    if isinstance(by, (list, tuple, set)):
        on = dict(zip(by, by))
    elif by is None:
        common_cols = intersect(x.columns.tolist(), y.columns)
        on = dict(zip(common_cols, common_cols))
    elif not isinstance(by, dict):
        on = {by: by} # type: ignore

    if copy:
        x = x.copy()

    def get_nested_df(row: Series) -> DataFrame:
        condition = None
        for key in on:
            if condition is None:
                condition = y[on[key]] == row[key]
            else:
                condition = condition & (y[on[key]] == row[key])
        df = y >> filter_(condition)
        if not keep:
            df = df[setdiff(df.columns, on.values())]

        return df

    y_matched = x.apply(get_nested_df, axis=1)
    y_name = name or getattr(y, "__dfname__", None)
    if y_name:
        y_matched = y_matched.to_frame(name=y_name)

    out = pandas.concat([x, y_matched], axis=1)

    if isinstance(x, DataFrameGroupBy):
        return x.__class__(
            x,
            _group_vars=group_vars(x),
            _group_drop=group_by_drop_default(x),
            _group_data=group_data(x),
        )
    return out
