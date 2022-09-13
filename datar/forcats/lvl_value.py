"""Provides forcats verbs to manipulate factor level values"""
from typing import Any, Callable, Iterable, List, Mapping

import numpy as np
from ..core.backends import pandas as pd
from ..core.backends.pandas import Categorical, DataFrame
from pipda import register_verb, Verb

from ..base import (
    levels,
    match,
    nlevels,
    paste0,
    sample,
    table,
    order,
    rank,
)
from ..core.contexts import Context
from ..core.utils import logger
from ..dplyr import recode_factor, if_else
from .utils import check_factor, ForcatsRegType
from .lvls import lvls_reorder, lvls_revalue
from .lvl_order import fct_relevel


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_anon(
    _f,
    prefix: str = "",
) -> Categorical:
    """Anonymise factor levels

    Args:
        f: A factor.
        prefix: A character prefix to insert in front of the random labels.

    Returns:
        The factor with levels anonymised
    """
    _f = check_factor(_f)
    nlvls = nlevels(_f)
    ndigits = len(str(nlvls))
    lvls = paste0(
        prefix,
        [str(i).rjust(ndigits, "0") for i in range(nlvls)],
    )
    _f = lvls_revalue(_f, sample(lvls), __ast_fallback="normal")
    return lvls_reorder(
        _f,
        match(lvls, levels(_f)),
        __ast_fallback="normal",
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_recode(
    _f,
    *args: Mapping[Any, Any],
    **kwargs: Any,
) -> Categorical:
    """Change factor levels by hand

    Args:
        _f: A factor
        *args: and
        **kwargs: A sequence of named character vectors where the name
            gives the new level, and the value gives the old level.
            Levels not otherwise mentioned will be left as is. Levels can
            be removed by naming them `NULL`.
            As `NULL/None` cannot be a name of keyword arguments, replacement
            has to be specified as a dict
            (i.e. `fct_recode(x, {NULL: "apple"})`)
            If you want to replace multiple values with the same old value,
            use a `set`/`list`/`numpy.ndarray`
            (i.e. `fct_recode(x, fruit=["apple", "banana"])`).
            This is a safe way, since `set`/`list`/`numpy.ndarray` is
            not hashable to be a level of a factor.
            Do NOT use a `tuple`, as it's hashable!

            Note that the order of the name-value is in the reverse way as
            `dplyr.recode()` and `dplyr.recode_factor()`

    Returns:
        The factor recoded with given recodings
    """
    _f = check_factor(_f)

    recodings = {}  # new => old
    for arg in args:
        if not isinstance(arg, dict):
            raise ValueError("`*args` have to be all mappings.")
        recodings.update(arg)
    recodings.update(kwargs)

    lvls = levels(_f)
    for_recode = dict(zip(lvls, lvls))  # old => new
    unknown = set()
    for key, val in recodings.items():
        if isinstance(val, (np.ndarray, set, list)):
            for value in val:
                if value not in lvls:
                    unknown.add(value)
                else:
                    for_recode[value] = key
        else:
            if val not in lvls:
                unknown.add(val)
            else:
                for_recode[val] = key

    if unknown:
        logger.warning("[fct_recode] Unknown levels in `_f`: %s", unknown)

    return recode_factor(_f, for_recode).values


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_collapse(
    _f,
    other_level: Any = None,
    **kwargs: List,
) -> Categorical:
    """Collapse factor levels into manually defined groups

    Args:
        _f: A factor
        **kwargs: The levels to collapse.
            Like `name=[old_level, old_level1, ...]`. The old levels will
            be replaced with `name`
        other_level: Replace all levels not named in `kwargs`.
            If not, don't collapse them.

    Returns:
        The factor with levels collapsed.
    """
    _f = check_factor(_f)
    levs = set(lev for sublevs in kwargs.values() for lev in sublevs)

    if other_level is not None:
        lvls = levels(_f)
        kwargs[other_level] = set(lvls) - levs

    out = fct_recode(_f, kwargs)
    if other_level in kwargs:
        return fct_relevel(
            out,
            other_level,
            after=-1,
            __ast_fallback="normal",
        )

    return out


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_min(
    _f,
    min: int,
    w=None,
    other_level: Any = "Other",
) -> Categorical:
    """lumps levels that appear fewer than `min` times.

    Args:
        _f: A factor
        min: Preserve levels that appear at least `min` number of times.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels lumped.
    """
    calcs = check_calc_levels(_f, w)
    _f = calcs["_f"]

    if min < 0:
        raise ValueError("`min` must be a positive number.")

    new_levels = if_else(
        calcs["count"] >= min,
        levels(_f),
        other_level,
    )

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __ast_fallback="normal")
        return fct_relevel(_f, other_level, after=-1)

    return _f


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_prop(
    _f,
    prop,
    w=None,
    other_level: Any = "Other",
) -> Categorical:
    """Lumps levels that appear in fewer `prop * n` times.

    Args:
        _f: A factor
        prop: Positive `prop` lumps values which do not appear at least
            `prop` of the time. Negative `prop` lumps values that
            do not appear at most `-prop` of the time.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels lumped.
    """
    calcs = check_calc_levels(_f, w)
    _f = calcs["_f"]

    prop_n = calcs["count"] / calcs["total"]
    if prop < 0:
        new_levels = if_else(
            prop_n <= -prop,
            levels(_f),
            other_level,
        )
    else:
        new_levels = if_else(
            prop_n > prop,
            levels(_f),
            other_level,
        )

    if prop > 0 and sum(prop_n <= prop) <= 1:
        return _f

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __ast_fallback="normal")
        return fct_relevel(_f, other_level, after=-1)

    return _f


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_n(
    _f,
    n: int,
    w=None,
    other_level: Any = "Other",
    ties_method: str = "min",
) -> Categorical:
    """Lumps all levels except for the `n` most frequent.

    Args:
        f: A factor
        n: Positive `n` preserves the most common `n` values.
            Negative `n` preserves the least common `-n` values.
            It there are ties, you will get at least `abs(n)` values.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.
        ties_method A character string specifying how ties are treated.
            One of: `average`, `first`, `dense`, `max`, and `min`.

    Returns:
        The factor with levels lumped.
    """
    calcs = check_calc_levels(_f, w)
    _f = calcs["_f"]

    if n < 0:
        rnk = rank(calcs["count"], ties_method=ties_method)
        n = -n
    else:
        rnk = rank(-calcs["count"], ties_method=ties_method)

    new_levels = if_else(
        rnk <= n,
        levels(_f),
        other_level,
    )

    if sum(rnk > n) <= 1:
        return _f

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __ast_fallback="normal")
        return fct_relevel(_f, other_level, after=-1)

    return _f  # pragma: no cover


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_lowfreq(_f, other_level: Any = "Other"):
    """lumps together the least frequent levels, ensuring
    that "other" is still the smallest level.

    Args:
        f: A factor
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels lumped.
    """
    calcs = check_calc_levels(_f)
    _f = calcs["_f"]

    new_levels = if_else(
        ~in_smallest(calcs["count"]),
        levels(_f),
        other_level,
    )

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __ast_fallback="normal")
        return fct_relevel(_f, other_level, after=-1)

    return _f


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump(
    _f,
    n: int = None,
    prop=None,
    w=None,
    other_level: Any = "Other",
    ties_method: str = "min",
) -> Categorical:
    """Lump together factor levels into "other"

    Args:
        f: A factor
        n: Positive `n` preserves the most common `n` values.
            Negative `n` preserves the least common `-n` values.
            It there are ties, you will get at least `abs(n)` values.
        prop: Positive `prop` lumps values which do not appear at least
            `prop` of the time. Negative `prop` lumps values that
            do not appear at most `-prop` of the time.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.
        ties_method A character string specifying how ties are treated.
            One of: `average`, `first`, `dense`, `max`, and `min`.

    Returns:
        The factor with levels lumped.
    """
    check_calc_levels(_f, w)

    if n is None and prop is None:
        return fct_lump_lowfreq(_f, other_level=other_level)
    if prop is None:
        return fct_lump_n(
            _f,
            n=n,
            w=w,
            other_level=other_level,
            ties_method=ties_method,
        )
    if n is None:
        return fct_lump_prop(_f, prop=prop, w=w, other_level=other_level)

    raise ValueError("Must supply only one of `n` and `prop`")


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_other(
    _f,
    keep: Iterable = None,
    drop: Iterable = None,
    other_level: Any = "Other",
) -> Categorical:
    """Replace levels with "other"

    Args:
        _f: A factor
        keep: and
        drop: Pick one of `keep` and `drop`:
            - `keep` will preserve listed levels, replacing all others with
                `other_level`.
            - `drop` will replace listed levels with `other_level`, keeping all
                as is.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels replaced.
    """
    _f = check_factor(_f)

    if (keep is None and drop is None) or (
        keep is not None and drop is not None
    ):
        raise ValueError("Must supply exactly one of `keep` and `drop`")

    lvls = levels(_f)
    if keep is not None:
        lvls[~np.isin(lvls, keep)] = other_level
    else:
        lvls[np.isin(lvls, drop)] = other_level

    _f = lvls_revalue(_f, lvls, __ast_fallback="normal")
    return fct_relevel(
        _f,
        other_level,
        after=-1,
        __ast_fallback="normal",
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_relabel(
    _f,
    _fun: Callable,
    *args: Any,
    **kwargs: Any,
) -> Categorical:
    """Automatically relabel factor levels, collapse as necessary

    Args:
        _f: A factor
        _fun: A function to be applied to each level. Must accept the old
            levels and return a character vector of the same length
            as its input.
        *args: and
        **kwargs: Addtional arguments to `_fun`

    Returns:
        The factor with levels relabeled
    """
    _f = check_factor(_f)
    old_levels = levels(_f)
    if isinstance(_fun, Verb):  # pragma: no cover
        # TODO: test
        kwargs["__ast_fallback"] = "normal"
    new_levels = _fun(old_levels, *args, **kwargs)
    return lvls_revalue(_f, new_levels, __ast_fallback="normal")


# -------------
# helpers
# -------------


def check_weights(w, n: int = None):
    """Check the weights"""
    if w is None:
        return w

    if n is None:  # pragma: no cover
        n = len(w)

    if len(w) != n:
        raise ValueError(
            f"`w` must be the same length as `f` ({n}), "
            f"not length {len(w)}."
        )

    for weight in w:
        if weight < 0 or pd.isnull(weight):
            raise ValueError(
                f"All `w` must be non-negative and non-missing, got {weight}."
            )

    return w


def check_calc_levels(_f, w=None):
    """Check levels to be calculated"""
    _f = check_factor(_f)
    w = check_weights(w, len(_f))

    if w is None:
        cnt = table(_f).iloc[0, :].values
        total = len(_f)
    else:
        cnt = (
            DataFrame({"w": w, "f": _f})
            .groupby("f", observed=False, sort=False, dropna=False)
            .agg("sum")
            .iloc[:, 0]
            .values
        )
        total = sum(w)

    return {"_f": _f, "count": cnt, "total": total}


def lump_cutoff(x) -> int:
    """Lump together smallest groups, ensuring that the collective
    "other" is still the smallest group. Assumes x is vector
    of counts in descending order"""
    left = sum(x)

    for i, elem in enumerate(x):
        # After group, there are this many left
        left -= elem

        if elem > left:
            return i + 1

    return len(x)  # pragma: no cover


def in_smallest(x) -> Iterable[bool]:
    """Check if elements in x are the smallest of x"""
    ord_x = order(x, decreasing=True)
    idx = lump_cutoff(x[ord_x])

    to_lump = np.arange(len(x)) >= idx
    return to_lump[order(ord_x)]
