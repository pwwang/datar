"""Provides forcats verbs to manipulate factor level values"""
from typing import Any, Callable, Iterable, List, Mapping, Union

import numpy
from pandas import Categorical, DataFrame
from pipda import register_verb
from pipda.utils import CallingEnvs, functype

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
from ..core.types import ForcatsRegType, ForcatsType, NumericType, is_null
from ..core.utils import get_option, logger
from ..dplyr import recode_factor, if_else
from .utils import check_factor
from .lvls import lvls_reorder, lvls_revalue
from .lvl_order import fct_relevel


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_anon(
    _f: ForcatsType,
    prefix: str = "",
    base0_: bool = None,
) -> Categorical:
    """Anonymise factor levels

    Args:
        f: A factor.
        prefix: A character prefix to insert in front of the random labels.
        base0_: Whether we start from 0?
            If not given, will use `get_option('index.base.0')`

    Returns:
        The factor with levels anonymised
    """
    _f = check_factor(_f)
    base0_ = get_option("index.base.0", base0_)
    nlvls = nlevels(_f, __calling_env=CallingEnvs.REGULAR)
    ndigits = len(str(nlvls - int(base0_)))
    lvls = paste0(
        prefix,
        [str(i + int(not base0_)).rjust(ndigits, "0") for i in range(nlvls)],
    )
    _f = lvls_revalue(
        _f,
        sample(lvls, __calling_env=CallingEnvs.REGULAR),
        __calling_env=CallingEnvs.REGULAR,
    )
    return lvls_reorder(
        _f,
        match(
            lvls,
            levels(_f, __calling_env=CallingEnvs.REGULAR),
            base0_=True,
            __calling_env=CallingEnvs.REGULAR,
        ),
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_recode(
    _f: ForcatsType,
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

    lvls = levels(_f, __calling_env=CallingEnvs.REGULAR)
    for_recode = dict(zip(lvls, lvls))  # old => new
    unknown = set()
    for key, val in recodings.items():
        if isinstance(val, (numpy.ndarray, set, list)):
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

    return recode_factor(_f, for_recode, __calling_env=CallingEnvs.REGULAR)


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_collapse(
    _f: ForcatsType,
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
        lvls = levels(_f, __calling_env=CallingEnvs.REGULAR)
        kwargs[other_level] = set(lvls) - levs

    out = fct_recode(_f, kwargs)
    if other_level in kwargs:
        return fct_relevel(
            out,
            other_level,
            after=-1,
            __calling_env=CallingEnvs.REGULAR,
        )

    return out


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_min(  # pylint: disable=redefined-builtin,invalid-name
    _f: ForcatsType,
    min: int,
    w: Iterable[NumericType] = None,
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
        levels(_f, __calling_env=CallingEnvs.REGULAR),
        other_level,
        __calling_env=CallingEnvs,
    )

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __calling_env=CallingEnvs.REGULAR)
        return fct_relevel(_f, other_level, after=-1)

    return _f


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_prop(  # pylint: disable=invalid-name
    _f: ForcatsType,
    prop: NumericType,
    w: Iterable[NumericType] = None,
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
            levels(_f, __calling_env=CallingEnvs.REGULAR),
            other_level,
            __calling_env=CallingEnvs.REGULAR,
        )
    else:
        new_levels = if_else(
            prop_n > prop,
            levels(_f, __calling_env=CallingEnvs.REGULAR),
            other_level,
            __calling_env=CallingEnvs.REGULAR,
        )

    if prop > 0 and sum(prop_n <= prop) <= 1:
        return _f

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __calling_env=CallingEnvs.REGULAR)
        return fct_relevel(_f, other_level, after=-1)

    return _f


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_n(  # pylint: disable=invalid-name
    _f: ForcatsType,
    n: int,
    w: Iterable[NumericType] = None,
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
        levels(_f, __calling_env=CallingEnvs.REGULAR),
        other_level,
        __calling_env=CallingEnvs.REGULAR,
    )

    if sum(rnk > n) <= 1:
        return _f

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __calling_env=CallingEnvs.REGULAR)
        return fct_relevel(_f, other_level, after=-1)

    return _f  # pragma: no cover


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump_lowfreq(_f: ForcatsType, other_level: Any = "Other"):
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
        levels(_f, __calling_env=CallingEnvs.REGULAR),
        other_level,
    )

    if other_level in new_levels:
        _f = lvls_revalue(_f, new_levels, __calling_env=CallingEnvs.REGULAR)
        return fct_relevel(_f, other_level, after=-1)

    return _f


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_lump(  # pylint: disable=invalid-name
    _f: ForcatsType,
    n: int = None,
    prop: NumericType = None,
    w: Iterable[NumericType] = None,
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
    _f: ForcatsType,
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

    lvls = levels(_f, __calling_env=CallingEnvs.REGULAR)
    if keep is not None:
        lvls[~numpy.isin(lvls, keep)] = other_level
    else:
        lvls[numpy.isin(lvls, drop)] = other_level

    _f = lvls_revalue(_f, lvls, __calling_env=CallingEnvs.REGULAR)
    return fct_relevel(
        _f,
        other_level,
        after=-1,
        __calling_env=CallingEnvs.REGULAR,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_relabel(
    _f: ForcatsType,
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
    old_levels = levels(_f, __calling_env=CallingEnvs.REGULAR)
    if functype(_fun) != 'plain':
        kwargs["__calling_env"] = CallingEnvs.REGULAR

    new_levels = _fun(old_levels, *args, **kwargs)
    return lvls_revalue(
        _f,
        new_levels,
        __calling_env=CallingEnvs.REGULAR
    )

# -------------
# helpers
# -------------


def check_weights(  # pylint: disable=invalid-name
    w: Iterable[NumericType],
    n: int = None,
) -> Iterable[NumericType]:
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
        if weight < 0 or is_null(weight):
            raise ValueError(
                f"All `w` must be non-negative and non-missing, got {weight}."
            )

    return w


def check_calc_levels(  # pylint: disable=invalid-name
    _f: ForcatsType,
    w: Iterable[NumericType] = None,
) -> Mapping[str, Union[ForcatsType, NumericType]]:
    """Check levels to be calculated"""
    _f = check_factor(_f)
    w = check_weights(w, len(_f))

    if w is None:
        cnt = table(_f).iloc[0, :]
        total = len(_f)
    else:
        cnt = (
            DataFrame({"w": w, "f": _f})
            .groupby("f", observed=False)
            .agg("sum")
            .iloc[:, 0]
        )
        total = sum(w)

    return {"_f": _f, "count": cnt, "total": total}


def lump_cutoff(x: Iterable[NumericType]) -> int:
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


def in_smallest(x: Iterable[NumericType]) -> Iterable[bool]:
    """Check if elements in x are the smallest of x"""
    ord_x = order(
        x,
        decreasing=True,
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )
    idx = lump_cutoff(x[ord_x])

    to_lump = numpy.arange(len(x)) >= idx
    return to_lump[order(ord_x, base0_=True, __calling_env=CallingEnvs.REGULAR)]
