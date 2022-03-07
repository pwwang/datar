"""Provides forcats verbs to manipulate factor level orders"""
from typing import Any, Callable, Iterable, Sequence

import pandas as pd
from pandas import Categorical, DataFrame, Series
from pandas.api.types import is_scalar
from pipda import register_func, register_verb
from pipda.utils import CallingEnvs, functype

from ..base import (
    NA,
    append,
    as_integer,
    duplicated,
    intersect,
    levels,
    match,
    median,
    nlevels,
    order,
    rev,
    sample,
    seq_len,
    setdiff,
    table,
)
from ..core.collections import Collection
from ..core.contexts import Context
from ..core.utils import logger, regcall
from .lvls import lvls_reorder, lvls_seq
from .utils import check_factor, ForcatsRegType


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_relevel(
    _f,
    *lvls: Any,
    after: int = None,
) -> Categorical:
    """Reorder factor levels by hand

    Args:
        _f: A factor (categoriccal), or a string vector
        *lvls: Either a function (then `len(lvls)` should equal to `1`) or
            the new levels.
            A function will be called with the current levels as input, and the
            return value (which must be a character vector) will be used to
            relevel the factor.
            Any levels not mentioned will be left in their existing order,
            by default after the explicitly mentioned levels.
        after: Where should the new values be placed?

    Returns:
        The factor with levels replaced
    """

    _f = check_factor(_f)
    old_levels = regcall(levels, _f)
    if len(lvls) == 1 and callable(lvls[0]):
        first_levels = lvls[0](old_levels)
    else:
        first_levels = Collection(lvls)

    unknown = regcall(setdiff, first_levels, old_levels)

    if len(unknown) > 0:
        logger.warning("[fct_relevel] Unknown levels in `_f`: %s", unknown)
        first_levels = regcall(intersect, first_levels, old_levels)

    new_levels = regcall(
        append,
        regcall(setdiff, old_levels, first_levels).astype(old_levels.dtype),
        first_levels,
        after=after,
    )

    return regcall(
        lvls_reorder,
        _f,
        regcall(match, new_levels, old_levels),
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_inorder(_f, ordered: bool = None) -> Categorical:
    """Reorder factor levels by first appearance

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    dups = regcall(duplicated, _f)
    idx = regcall(as_integer, _f)[~dups]
    idx = idx[~pd.isnull(_f[~dups])]
    return regcall(lvls_reorder, _f, idx, ordered=ordered)


as_factor = fct_inorder


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_infreq(_f, ordered: bool = None) -> Categorical:
    """Reorder factor levels by frequency

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    return regcall(
        lvls_reorder,
        _f,
        regcall(
            order,
            regcall(table, _f).values.flatten(),
            decreasing=True,
        ),
        ordered=ordered,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_inseq(_f, ordered: bool = None) -> Categorical:
    """Reorder factor levels by numeric order

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    levs = regcall(levels, _f)
    num_levels = []
    for lev in levs:
        try:
            numlev = regcall(as_integer, lev)
        except (ValueError, TypeError):
            numlev = NA
        num_levels.append(numlev)

    if all(pd.isnull(num_levels)):
        raise ValueError(
            "At least one existing level must be coercible to numeric."
        )

    return regcall(
        lvls_reorder,
        _f,
        regcall(order, num_levels, na_last=True),
        ordered=ordered,
    )


@register_func(None, context=Context.EVAL)
def last2(_x: Iterable, _y: Sequence) -> Any:
    """Find the last element of `_y` ordered by `_x`

    Args:
        _x: The vector used to order `_y`
        _y: The vector to get the last element of

    Returns:
        Last element of `_y` ordered by `_x`
    """
    return list(_y[order(_x, na_last=False)])[-1]


@register_func(None, context=Context.EVAL)
def first2(_x: Sequence, _y: Sequence) -> Any:
    """Find the first element of `_y` ordered by `_x`

    Args:
        _x: The vector used to order `_y`
        _y: The vector to get the first element of

    Returns:
        First element of `_y` ordered by `_x`
    """
    return _y[order(_x)][0]


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_reorder(
    _f,
    _x: Sequence,
    *args: Any,
    _fun: Callable = median,
    _desc: bool = False,
    **kwargs: Any,
) -> Categorical:
    """Reorder factor levels by sorting along another variable

    Args:
        _f: A factor
        _x: The levels of `f` are reordered so that the values
            of `_fun(_x)` are in ascending order.
        _fun: The summary function, have to be passed by keyword
        *args, **kwargs: Other arguments for `_fun`.
        _desc: Order in descending order?

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    if is_scalar(_x):
        _x = [_x]

    if len(_f) != len(_x):
        raise ValueError("Unmatched length between `_x` and `_f`.")

    if functype(_fun) != "plain":
        kwargs["__calling_env"] = CallingEnvs.REGULAR

    args = args[1:]
    # simulate tapply
    summary = (
        DataFrame({"f": _f, "x": _x})
        .groupby("f", observed=False)
        .agg(lambda col: _fun(col, *args, **kwargs))
    )

    if not is_scalar(summary.iloc[0, 0]):
        raise ValueError("`fun` must return a single value per group.")

    return regcall(
        lvls_reorder,
        _f,
        regcall(order, summary.iloc[:, 0], decreasing=_desc),
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_reorder2(
    _f,
    _x: Sequence,
    _y: Sequence,
    *args: Any,
    _fun: Callable = last2,
    _desc: bool = True,
    **kwargs: Any,
) -> Categorical:
    """Reorder factor levels by sorting along another variable

    Args:
        _f: A factor
        _x: and
        _y: The levels of `f` are reordered so that the values
            of `_fun(_x, _y)` are in ascending order.
        _fun: The summary function, have to be passed by keyword
        *args, **kwargs: Other arguments for `_fun`.
        _desc: Order in descending order?

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    if is_scalar(_x):
        _x = [_x]
    if is_scalar(_y):
        _y = [_y]
    if len(_f) != len(_x) or len(_f) != len(_y):
        raise ValueError("Unmatched length between `_x` and `_f`.")

    if functype(_fun) != "plain":
        kwargs["__calling_env"] = CallingEnvs.REGULAR

    args = args[1:]
    # simulate tapply
    summary = (
        DataFrame({"f": _f, "x": _x, "y": _y})
        .groupby("f", observed=False)
        .apply(
            lambda row: _fun(
                row.x.reset_index(drop=True),
                row.y.reset_index(drop=True),
                *args,
                **kwargs,
            )
        )
    )

    if not isinstance(summary, Series) or not is_scalar(summary.values[0]):
        raise ValueError("`fun` must return a single value per group.")

    return regcall(
        lvls_reorder,
        _f,
        regcall(order, summary, decreasing=_desc),
    )


@register_verb(ForcatsRegType)
def fct_shuffle(_f) -> Categorical:
    """Randomly permute factor levels

    Args:
        _f: A factor

    Returns:
        The factor with levels randomly permutated
    """
    _f = check_factor(_f)

    return regcall(lvls_reorder, _f, regcall(sample, lvls_seq(_f)))


@register_verb(ForcatsRegType)
def fct_rev(_f) -> Categorical:
    """Reverse order of factor levels

    Args:
        _f: A factor

    Returns:
        The factor with levels reversely ordered
    """
    _f = check_factor(_f)

    return regcall(lvls_reorder, _f, regcall(rev, lvls_seq(_f)))


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_shift(_f, n: int = 1) -> Categorical:
    """Shift factor levels to left or right, wrapping around at end

    Args:
        f: A factor.
        n: Positive values shift to the left; negative values shift to
            the right.

    Returns:
        The factor with levels shifted
    """
    nlvls = regcall(nlevels, _f)
    lvl_order = (regcall(seq_len, nlvls) + n - 1) % nlvls

    return regcall(lvls_reorder, _f, lvl_order)
