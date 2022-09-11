"""Mutating joins"""
from pipda import register_verb

from ..core.backends import pandas as pd
from ..core.backends.pandas import Categorical, DataFrame
from ..core.backends.pandas.api.types import is_scalar, is_categorical_dtype

from ..core.contexts import Context
from ..core.tibble import reconstruct_tibble
from ..base import intersect, setdiff, union
from .dfilter import filter as filter_


def _join(
    x,
    y,
    how,
    by=None,
    copy=False,
    suffix=("_x", "_y"),
    # na_matches = "", # TODO: how?
    keep=False,
):
    """General join"""
    # make sure df.x returns a Series not SeriesGroupBy for TibbleGrouped
    newx = DataFrame(x, copy=False)
    y = DataFrame(y, copy=False)

    if by is not None and not by:
        ret = pd.merge(newx, y, how="cross", copy=copy, suffixes=suffix)

    elif isinstance(by, dict):
        left_on = list(by)
        right_on = list(by.values())
        ret = pd.merge(
            newx,
            y,
            left_on=left_on,
            right_on=right_on,
            how=how,
            copy=copy,
            suffixes=suffix,
        )
        if not keep:
            to_drop = setdiff(right_on, left_on, __ast_fallback="normal")
            ret.drop(columns=to_drop, inplace=True)

    elif keep:
        if by is None:
            by = intersect(newx.columns, y.columns, __ast_fallback="normal")
        # on=... doesn't keep both by columns in left and right
        left_on = [f"{col}{suffix[0]}" for col in by]
        right_on = [f"{col}{suffix[1]}" for col in by]
        newx = newx.rename(columns=dict(zip(by, left_on)))
        y = y.rename(columns=dict(zip(by, right_on)))
        ret = pd.merge(
            newx,
            y,
            left_on=left_on,
            right_on=right_on,
            how=how,
            copy=copy,
            suffixes=suffix,
        )

    else:
        if by is None:
            by = intersect(newx.columns, y.columns, __ast_fallback="normal")

        by = [by] if is_scalar(by) else list(by)
        ret = pd.merge(newx, y, on=by, how=how, copy=copy, suffixes=suffix)
        for col in by:
            # try recovering factor columns
            if is_categorical_dtype(x[col]) and is_categorical_dtype(y[col]):
                ret[col] = Categorical(
                    ret[col],
                    categories=union(
                        x[col].cat.categories,
                        y[col].cat.categories,
                        __ast_fallback="normal",
                    ),
                )

    return reconstruct_tibble(x, ret)


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"by": Context.SELECT},
)
def inner_join(
    x,
    y,
    by=None,
    copy=False,
    suffix=("_x", "_y"),
    keep=False,
):
    """Mutating joins including all rows in x and y.

    Args:
        x, y: A pair of data frames
        by: A character vector of variables to join by.
            If keys from `x` and `y` are different, use a dict
            (i.e. `{"colX": "colY"}`) instead of a list.
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
        x,
        y,
        how="inner",
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep,
    )


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"by": Context.SELECT},
)
def left_join(
    x,
    y,
    by=None,
    copy=False,
    suffix=("_x", "_y"),
    keep=False,
):
    """Mutating joins including all rows in x.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    return _join(
        x,
        y,
        how="left",
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep,
    )


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"by": Context.SELECT},
)
def right_join(
    x,
    y,
    by=None,
    copy=False,
    suffix=("_x", "_y"),
    keep=False,
):
    """Mutating joins including all rows in y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)

    Note:
        The rows of the order is preserved according to `y`. But `dplyr`'s
        `right_join` preserves order from `x`.
    """
    return _join(
        x,
        y,
        how="right",
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep,
    )


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"by": Context.SELECT},
)
def full_join(
    x,
    y,
    by=None,
    copy=False,
    suffix=("_x", "_y"),
    keep=False,
):
    """Mutating joins including all rows in x or y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    return _join(
        x,
        y,
        how="outer",
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep,
    )


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"by": Context.SELECT},
)
def semi_join(
    x,
    y,
    by=None,
    copy=False,
):
    """Returns all rows from x with a match in y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    on = _merge_on(by)
    right_on = on.get("right_on", on.get("on", y.columns))

    ret = pd.merge(
        DataFrame(x, copy=False),
        # fix #71: semi_join returns duplicated rows
        DataFrame(y, copy=False).drop_duplicates(right_on),
        how="left",
        copy=copy,
        suffixes=["", "_y"],
        indicator="__merge__",
        **on,
    )
    ret = ret.loc[ret["__merge__"] == "both", x.columns]
    return reconstruct_tibble(x, ret)


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"by": Context.SELECT},
)
def anti_join(
    x,
    y,
    by=None,
    copy=False,
):
    """Returns all rows from x without a match in y.

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    ret = pd.merge(
        DataFrame(x, copy=False),
        DataFrame(y, copy=False),
        how="left",
        copy=copy,
        suffixes=["", "_y"],
        indicator=True,
        **_merge_on(by),
    )
    ret = ret.loc[ret._merge != "both", x.columns]
    return reconstruct_tibble(x, ret)


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"by": Context.SELECT},
)
def nest_join(
    x,
    y,
    by=None,
    copy=False,
    keep=False,
    name=None,
):
    """Returns all rows and columns in x with a new nested-df column that
    contains all matches from y

    See Also:
        [`inner_join()`](datar.dplyr.join.inner_join)
    """
    on = by
    newx = DataFrame(x, copy=False)
    y = DataFrame(y, copy=False)
    if isinstance(by, (list, tuple, set)):
        on = dict(zip(by, by))
    elif by is None:
        common_cols = intersect(
            newx.columns,
            y.columns,
            __ast_fallback="normal",
        )
        on = dict(zip(common_cols, common_cols))
    elif not isinstance(by, dict):
        on = {by: by}

    if copy:
        newx = newx.copy()

    def get_nested_df(row):
        row = getattr(row, "obj", row)
        condition = None
        for key in on:
            if condition is None:
                condition = y[on[key]] == row[key]
            else:
                condition = condition & (y[on[key]] == row[key])
        df = filter_(y, condition, __ast_fallback="normal")
        if not keep:
            df = df[
                setdiff(df.columns, list(on.values()), __ast_fallback="normal")
            ]

        return df

    y_matched = newx.apply(get_nested_df, axis=1)
    y_name = name or "_y_joined"
    if y_name:
        y_matched = y_matched.to_frame(name=y_name)

    out = pd.concat([newx, y_matched], axis=1)
    return reconstruct_tibble(x, out)


def _merge_on(by):
    """Calculate argument on for pandas.merge()"""
    if by is None:
        return {}
    if isinstance(by, dict):
        return {"left_on": list(by), "right_on": list(by.values())}
    return {"on": by}
