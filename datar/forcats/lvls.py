"""Lower-level APIs to manipulate the factors"""
from typing import Iterable, List
from pandas import Categorical
from pipda import register_verb

from ..core.utils import regcall
from ..core.contexts import Context
from ..base import setdiff, union

from ..base import (
    NA,
    as_character,
    as_integer,
    setequal,
    seq_along,
    is_integer,
    factor,
    levels,
    match,
    nlevels,
    is_ordered,
    unique,
)

from ..dplyr import recode_factor

from .utils import check_factor, ForcatsRegType


def lvls_seq(_f):
    """Get the index sequence of a factor levels"""
    return regcall(seq_along, regcall(levels, _f)) - 1


def refactor(_f, new_levels: Iterable, ordered: bool = None) -> Categorical:
    """Refactor using new levels"""
    if ordered is None:
        ordered = regcall(is_ordered, _f)

    new_f = factor(_f, levels=new_levels, exclude=NA, ordered=ordered)
    # keep attributes?
    return new_f


@register_verb(ForcatsRegType, context=Context.EVAL)
def lvls_reorder(
    _f,
    idx,
    ordered: bool = None,
) -> Categorical:
    """Leaves values of a factor as they are, but changes the order by
    given indices

    Args:
        f: A factor (or character vector).
        idx: A integer index, with one integer for each existing level.
        new_levels: A character vector of new levels.
        ordered: A logical which determines the "ordered" status of the
          output factor. `None` preserves the existing status of the factor.

    Returns:
        The factor with levels reordered
    """
    _f = check_factor(_f)
    if not regcall(is_integer, idx):
        raise ValueError("`idx` must be integers")

    len_idx = len(idx)
    seq_lvls = lvls_seq(_f)
    if not regcall(setequal, idx, seq_lvls) or len_idx != regcall(nlevels, _f):
        raise ValueError(
            "`idx` must contain one integer for each level of `f`"
        )

    return refactor(
        _f,
        regcall(levels, _f)[idx],
        ordered=ordered,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def lvls_revalue(
    _f,
    new_levels: Iterable,
) -> Categorical:
    """changes the values of existing levels; there must
    be one new level for each old level

    Args:
        _f: A factor
        new_levels: A character vector of new levels.

    Returns:
        The factor with the new levels
    """
    _f = check_factor(_f)

    if len(new_levels) != regcall(nlevels, _f):
        raise ValueError(
            "`new_levels` must be the same length as `levels(f)`: expected ",
            f"{regcall(nlevels, _f)} new levels, " f"got {len(new_levels)}.",
        )

    u_levels = regcall(unique, new_levels)
    if len(new_levels) > len(u_levels):
        # has duplicates
        index = regcall(match, new_levels, u_levels)
        out = factor(regcall(as_character, index[regcall(as_integer, _f)]))
        return regcall(
            recode_factor,
            out,
            dict(zip(regcall(levels, out), u_levels)),
        ).values

    recodings = dict(zip(regcall(levels, _f), new_levels))
    return regcall(recode_factor, _f, recodings).values


@register_verb(ForcatsRegType, context=Context.EVAL)
def lvls_expand(
    _f,
    new_levels: Iterable,
) -> Categorical:
    """Expands the set of levels; the new levels must
    include the old levels.

    Args:
        _f: A factor
        new_levels: The new levels. Must include the old ones

    Returns:
        The factor with the new levels
    """
    _f = check_factor(_f)
    levs = regcall(levels, _f)

    missing = regcall(setdiff, levs, new_levels)
    if len(missing) > 0:
        raise ValueError(
            "Must include all existing levels. Missing: {missing}"
        )

    return refactor(_f, new_levels=new_levels)


@register_verb(ForcatsRegType)
def lvls_union(
    fs,
) -> List:
    """Find all levels in a list of factors

    Args:
        fs: A list of factors

    Returns:
        A list of all levels
    """
    out = []
    for fct in fs:
        fct = check_factor(fct)
        levs = regcall(levels, fct)
        out = regcall(union, out, levs)
    return out
