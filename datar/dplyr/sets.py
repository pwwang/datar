"""Set operations

https://github.com/tidyverse/dplyr/blob/master/R/sets.r
"""
from pipda import register_verb

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame

from ..core.contexts import Context
from ..core.tibble import TibbleGrouped, reconstruct_tibble

from ..base.verbs import intersect, union, setdiff, setequal
from .bind import bind_rows
from .group_by import ungroup


def _check_xy(x, y):
    """Check the dimension and columns of x and y for set operations"""
    if x.shape[1] != y.shape[1]:
        raise ValueError(
            "not compatible:\n"
            f"- different number of columns: {x.shape[1]} vs {y.shape[1]}"
        )

    in_y_not_x = setdiff(y.columns, x.columns, __ast_fallback="normal")
    in_x_not_y = setdiff(x.columns, y.columns, __ast_fallback="normal")
    if in_y_not_x.size > 0 or in_x_not_y.size > 0:
        msg = ["not compatible:"]
        if in_y_not_x:
            msg.append(f"- Cols in `y` but not `x`: {in_y_not_x}.")
        if in_x_not_y:
            msg.append(f"- Cols in `x` but not `y`: {in_x_not_y}.")
        raise ValueError("\n".join(msg))


@intersect.register(DataFrame, context=Context.EVAL)
def _(x: DataFrame, y: DataFrame) -> DataFrame:
    """Intersect of two dataframes

    Args:
        _data, data2, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of intersect of input dataframes
    """
    _check_xy(x, y)
    from .distinct import distinct

    out = distinct(
        pd.merge(x, ungroup(y, __ast_fallback="normal"), how="inner"),
        __ast_fallback="normal",
    )
    if isinstance(y, TibbleGrouped):
        return reconstruct_tibble(y, out)
    return out


@intersect.register(TibbleGrouped, context=Context.EVAL)
def _(x, y):
    newx = ungroup(x, __ast_fallback="normal")
    newy = ungroup(y, __ast_fallback="normal")
    out = intersect.dispatch(DataFrame)(newx, newy)
    return reconstruct_tibble(x, out)


@union.register(DataFrame, context=Context.EVAL)
def _(x, y):
    """Union of two dataframes, ignoring grouping structure and indxes

    Args:
        x: and
        y: Dataframes to perform operations

    Returns:
        The dataframe of union of input dataframes
    """
    _check_xy(x, y)
    from .distinct import distinct

    out = distinct(
        pd.merge(x, ungroup(y, __ast_fallback="normal"), how="outer"),
        __ast_fallback="normal",
    )
    out.reset_index(drop=True, inplace=True)
    if isinstance(y, TibbleGrouped):
        return reconstruct_tibble(y, out)
    return out


@union.register(TibbleGrouped, context=Context.EVAL)
def _(x, y):
    out = union.dispatch(DataFrame)(
        ungroup(x, __ast_fallback="normal"),
        ungroup(y, __ast_fallback="normal"),
    )
    return reconstruct_tibble(x, out)


@setdiff.register(DataFrame, context=Context.EVAL)
def _(x, y):
    """Set diff of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of setdiff of input dataframes
    """
    _check_xy(x, y)
    indicator = "__datar_setdiff__"
    out = pd.merge(
        x,
        ungroup(y, __ast_fallback="normal"),
        how="left",
        indicator=indicator,
    )

    from .distinct import distinct

    out = distinct(
        out[out[indicator] == "left_only"]
        .drop(columns=[indicator])
        .reset_index(drop=True),
        __ast_fallback="normal",
    )
    if isinstance(y, TibbleGrouped):
        return reconstruct_tibble(y, out)
    return out


@setdiff.register(TibbleGrouped, context=Context.EVAL)
def _(x, y):
    out = setdiff.dispatch(DataFrame)(
        ungroup(x, __ast_fallback="normal"),
        ungroup(y, __ast_fallback="normal"),
    )
    return reconstruct_tibble(x, out)


@register_verb(DataFrame, context=Context.EVAL)
def union_all(x, y):
    """Union of all rows of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of union of all rows of input dataframes
    """
    _check_xy(x, y)
    out = bind_rows(
        x,
        ungroup(y, __ast_fallback="normal"),
        __ast_fallback="normal",
    )
    if isinstance(y, TibbleGrouped):
        return reconstruct_tibble(y, out)
    return out


@union_all.register(TibbleGrouped, context=Context.EVAL)
def _(x, y):
    out = union_all.dispatch(DataFrame)(
        ungroup(x, __ast_fallback="normal"),
        ungroup(y, __ast_fallback="normal"),
    )
    return reconstruct_tibble(x, out)


@setequal.register(DataFrame, context=Context.EVAL)
def _(x, y, equal_na=True):
    """Check if two dataframes equal, grouping structures are ignored.

    Args:
        x: The first dataframe
        y: The second dataframe
        equal_na: To be compatible with non-dataframe version. Takes no effect
            for dataframe.

    Returns:
        True if they equal else False
    """
    x = ungroup(x, __ast_fallback="normal")
    y = ungroup(y, __ast_fallback="normal")
    _check_xy(x, y)

    x = x.sort_values(by=x.columns.to_list()).reset_index(drop=True)
    y = y.sort_values(by=y.columns.to_list()).reset_index(drop=True)
    return x.equals(y)
