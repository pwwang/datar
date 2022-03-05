"""Provides functions for multiple factors"""
import itertools

from pandas import Categorical

from ..core.utils import regcall
from ..base import factor, paste, levels, expandgrid, intersect
from .utils import check_factor
from .lvls import lvls_union


def fct_c(*fs) -> Categorical:
    """Concatenate factors, combining levels

    This is a useful way of patching together factors from multiple sources
    that really should have the same levels but don't.

    Args:
        fs: The factors to be concatenated

    Returns:
        The concatenated factor
    """
    if not fs:
        return factor()

    levs = lvls_union(fs)
    allvals = itertools.chain(*fs)
    return factor(allvals, levels=levs, exclude=None)


def fct_cross(
    *fs,
    sep: str = ":",
    keep_empty: bool = False,
) -> Categorical:
    """Combine levels from two or more factors to create a new factor

    Computes a factor whose levels are all the combinations of
    the levels of the input factors.

    Args:
        *fs: Factors to combine
        sep: A string to separate levels
        keep_empty: If True, keep combinations with no observations as levels

    Returns:
        The new factor
    """
    if not fs:
        return factor()

    fs = [check_factor(fct) for fct in fs]
    newf = regcall(paste, *fs, sep=sep)

    old_levels = (regcall(levels, fct) for fct in fs)
    grid = regcall(expandgrid, *old_levels)
    new_levels = regcall(
        paste,
        *(grid[col] for col in grid),
        sep=sep,
    )

    if not keep_empty:
        new_levels = regcall(intersect, new_levels, newf)

    return factor(newf, levels=new_levels)
