"""Lower-level APIs to manipulate the factors"""
from typing import Iterable, List
from pandas import Categorical
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.types import IntType, ForcatsRegType, ForcatsType
from ..core.utils import position_at
from ..core.contexts import Context
from ..base import setdiff, union

from ..base import (
    NA,
    as_character,
    as_integer,
    setequal,
    seq_along,
    factor,
    levels,
    match,
    nlevels,
    is_ordered,
    is_integer,
    unique,
)

from ..dplyr import recode_factor

from .utils import check_factor


def lvls_seq(_f: ForcatsType, base0_: bool = None):
    """Get the index sequence of a factor levels"""
    return seq_along(
        levels(_f, __calling_env=CallingEnvs.REGULAR),
        base0_=base0_,
        __calling_env=CallingEnvs.REGULAR,
    )


def refactor(_f, new_levels: Iterable, ordered: bool = None) -> Categorical:
    """Refactor using new levels"""
    if ordered is None:
        ordered = is_ordered(_f, __calling_env=CallingEnvs.REGULAR)

    new_f = factor(_f, levels=new_levels, exclude=NA, ordered=ordered)
    # keep attributes?
    return new_f


@register_verb(ForcatsRegType, context=Context.EVAL)
def lvls_reorder(
    _f: ForcatsType,
    idx: Iterable[IntType],
    ordered: bool = None,
    base0_: bool = None,
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
    if not is_integer(idx, __calling_env=CallingEnvs.REGULAR):
        raise ValueError("`idx` must be integers")

    len_idx = len(idx)
    seq_lvls = lvls_seq(_f, base0_=base0_)
    if not setequal(
        idx, seq_lvls, __calling_env=CallingEnvs.REGULAR
    ) or len_idx != nlevels(_f, __calling_env=CallingEnvs.REGULAR):
        raise ValueError("`idx` must contain one integer for each level of `f`")

    pos = [position_at(index, len_idx, base0=base0_) for index in idx]
    return refactor(
        _f,
        levels(_f, __calling_env=CallingEnvs.REGULAR)[pos],
        ordered=ordered,
    )


@register_verb(ForcatsRegType, context=Context.EVAL)
def lvls_revalue(
    _f: ForcatsType,
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

    if len(new_levels) != nlevels(_f, __calling_env=CallingEnvs.REGULAR):
        raise ValueError(
            "`new_levels` must be the same length as `levels(f)`: expected ",
            f"{nlevels(_f, __calling_env=CallingEnvs.REGULAR)} new levels, "
            f"got {len(new_levels)}.",
        )

    u_levels = unique(new_levels, __calling_env=CallingEnvs.REGULAR)
    if len(new_levels) > len(u_levels):
        # has duplicates
        index = match(new_levels, u_levels, __calling_env=CallingEnvs.REGULAR)
        out = factor(
            as_character(
                index[
                    as_integer(
                        _f,
                        base0_=True,
                        __calling_env=CallingEnvs.REGULAR,
                    )
                ],
                __calling_env=CallingEnvs.REGULAR,
            )
        )
        return recode_factor(
            out,
            dict(
                zip(
                    levels(out, __calling_env=CallingEnvs.REGULAR),
                    u_levels,
                )
            ),
            __calling_env=CallingEnvs.REGULAR,
        )

    recodings = dict(
        zip(levels(_f, __calling_env=CallingEnvs.REGULAR), new_levels)
    )
    return recode_factor(_f, recodings, __calling_env=CallingEnvs.REGULAR)


@register_verb(ForcatsRegType, context=Context.EVAL)
def lvls_expand(
    _f: ForcatsType,
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
    levs = levels(_f, __calling_env=CallingEnvs.REGULAR)

    missing = setdiff(levs, new_levels, __calling_env=CallingEnvs.REGULAR)
    if len(missing) > 0:
        raise ValueError("Must include all existing levels. Missing: {missing}")

    return refactor(_f, new_levels=new_levels)


@register_verb(ForcatsRegType)
def lvls_union(
    fs: Iterable[ForcatsType],
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
        levs = levels(fct, __calling_env=CallingEnvs.REGULAR)
        out = union(out, levs, __calling_env=CallingEnvs.REGULAR)
    return out
