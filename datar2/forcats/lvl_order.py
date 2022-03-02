"""Provides forcats verbs to manipulate factor level orders"""
from typing import Any, Callable, Iterable, Sequence

from pandas import Categorical, DataFrame, Series
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
    table
)
from ..core.contexts import Context
from ..core.types import (
    ForcatsRegType,
    ForcatsType,
    is_not_null,
    is_null,
    is_scalar
)
from ..core.utils import get_option, logger
from .lvls import lvls_reorder, lvls_seq
from .utils import check_factor


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_relevel(
    _f: ForcatsType,
    *lvls: Any,
    after: int = 0,
    base0_: bool = None,
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
        base0_: Whether after is 0-based.
            if not given, will use `get_option("index.base.0")`.
            When it's 1-based, after=0 will append to the beginning,
            -1 will append to the end.
            When 0-based, after=None will append to the beginning,
            -1 to the end

    Returns:
        The factor with levels replaced
    """

    _f = check_factor(_f)
    old_levels = levels(_f, __calling_env=CallingEnvs.REGULAR)
    if len(lvls) == 1 and callable(lvls[0]):
        first_levels = lvls[0](old_levels)
    else:
        first_levels = lvls

    unknown = setdiff(
        first_levels,
        old_levels,
        __calling_env=CallingEnvs.REGULAR,
    )

    if len(unknown) > 0:
        logger.warning("[fct_relevel] Unknown levels in `_f`: %s", unknown)
        first_levels = intersect(
            first_levels,
            old_levels,
            __calling_env=CallingEnvs.REGULAR,
        )

    new_levels = append(
        setdiff(old_levels, first_levels, __calling_env=CallingEnvs.REGULAR),
        first_levels,
        after=after,
        __calling_env=CallingEnvs.REGULAR,
    )

    base0_ = get_option("index.base.0", base0_)
    return lvls_reorder(
        _f,
        match(  # base0_ defaults to which.base.0
            new_levels,
            old_levels,
            base0_=base0_,
            __calling_env=CallingEnvs.REGULAR,
        ),
        base0_=base0_,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_inorder(_f: ForcatsType, ordered: bool = None) -> Categorical:
    """Reorder factor levels by first appearance

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    dups = duplicated(_f, __calling_env=CallingEnvs.REGULAR)
    idx = as_integer(_f, __calling_env=CallingEnvs.REGULAR)[~dups]
    idx = idx[is_not_null(_f[~dups])]
    return lvls_reorder(
        _f,
        idx,
        ordered=ordered,
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


as_factor = fct_inorder


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_infreq(_f: ForcatsType, ordered: bool = None) -> Categorical:
    """Reorder factor levels by frequency

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    return lvls_reorder(
        _f,
        order(
            table(_f, __calling_env=CallingEnvs.REGULAR).values.flatten(),
            decreasing=True,
            base0_=True,
            __calling_env=CallingEnvs.REGULAR,
        ),
        ordered=ordered,
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_inseq(_f: ForcatsType, ordered: bool = None) -> Categorical:
    """Reorder factor levels by numeric order

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    levs = levels(_f, __calling_env=CallingEnvs.REGULAR)
    num_levels = []
    for lev in levs:
        try:
            numlev = as_integer(lev, __calling_env=CallingEnvs.REGULAR)
        except (ValueError, TypeError):
            numlev = NA
        num_levels.append(numlev)

    if all(is_null(num_levels)):
        raise ValueError(
            "At least one existing level must be coercible to numeric."
        )

    return lvls_reorder(
        _f,
        order(
            num_levels,
            na_last=True,
            base0_=True,
            __calling_env=CallingEnvs.REGULAR,
        ),
        ordered=ordered,
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
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
    return list(_y[order(_x, na_last=False, base0_=True)])[-1]


@register_func(None, context=Context.EVAL)
def first2(_x: Sequence, _y: Sequence) -> Any:
    """Find the first element of `_y` ordered by `_x`

    Args:
        _x: The vector used to order `_y`
        _y: The vector to get the first element of

    Returns:
        First element of `_y` ordered by `_x`
    """
    return _y[order(_x, base0_=True)][0]


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_reorder(
    _f: ForcatsType,
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

    return lvls_reorder(
        _f,
        order(
            summary.iloc[:, 0],
            decreasing=_desc,
            base0_=True,
            __calling_env=CallingEnvs.REGULAR,
        ),
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_reorder2(
    _f: ForcatsType,
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

    if not isinstance(summary, Series) or not is_scalar(summary[0]):
        raise ValueError("`fun` must return a single value per group.")

    return lvls_reorder(
        _f,
        order(
            summary,
            decreasing=_desc,
            base0_=True,
            __calling_env=CallingEnvs.REGULAR,
        ),
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType)
def fct_shuffle(_f: ForcatsType) -> Categorical:
    """Randomly permute factor levels

    Args:
        _f: A factor

    Returns:
        The factor with levels randomly permutated
    """
    _f = check_factor(_f)

    return lvls_reorder(
        _f,
        sample(lvls_seq(_f, base0_=True), __calling_env=CallingEnvs.REGULAR),
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType)
def fct_rev(_f: ForcatsType) -> Categorical:
    """Reverse order of factor levels

    Args:
        _f: A factor

    Returns:
        The factor with levels reversely ordered
    """
    _f = check_factor(_f)

    return lvls_reorder(
        _f,
        rev(lvls_seq(_f, base0_=True), __calling_env=CallingEnvs.REGULAR),
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_shift(_f: ForcatsType, n: int = 1) -> Categorical:
    """Shift factor levels to left or right, wrapping around at end

    Args:
        f: A factor.
        n: Positive values shift to the left; negative values shift to
            the right.

    Returns:
        The factor with levels shifted
    """
    nlvls = nlevels(_f, __calling_env=CallingEnvs.REGULAR)
    lvl_order = (seq_len(nlvls, base0_=True) + n) % nlvls

    return lvls_reorder(
        _f,
        lvl_order,
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )
