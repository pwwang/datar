"""Provides other helper functions for factors"""
from typing import Any, Iterable
import numpy
from pandas import Categorical, DataFrame
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.types import ForcatsRegType, ForcatsType, is_null, is_scalar
from ..core.utils import Array
from ..core.contexts import Context

from ..core.defaults import f
from ..base import (
    factor,
    tabulate,
    prop_table,
    nlevels,
    levels,
    NA,
    setdiff,
    is_ordered,
)
from ..dplyr import arrange, desc, mutate

from .utils import check_factor
from .lvl_order import fct_inorder


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_count(_f: ForcatsType, sort: bool = False, prop=False) -> Categorical:
    """Count entries in a factor

    Args:
        _f: A factor
        sort: If True, sort the result so that the most common values float to
            the top
        prop: If True, compute the fraction of marginal table.

    Returns:
        A data frame with columns `f`, `n` and `p`, if prop is True
    """
    f2 = check_factor(_f)
    n_na = sum(is_null(f2))

    df = DataFrame(
        {
            "f": fct_inorder(
                levels(f2, __calling_env=CallingEnvs.REGULAR),
                __calling_env=CallingEnvs.REGULAR,
            ),
            "n": tabulate(
                f2,
                nlevels(f2, __calling_env=CallingEnvs.REGULAR),
                __calling_env=CallingEnvs.REGULAR,
            ),
        }
    )

    if n_na > 0:
        df = df.append({"f": NA, "n": n_na}, ignore_index=True)

    if sort:
        df = arrange(
            df,
            desc(f.n, __calling_env=CallingEnvs.PIPING),
            __calling_env=CallingEnvs.REGULAR,
        )
    if prop:
        df = mutate(
            df,
            p=prop_table(f.n, __calling_env=CallingEnvs.PIPING),
            __calling_env=CallingEnvs.REGULAR,
        )

    return df


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_match(_f: ForcatsType, lvls: Any) -> Iterable[bool]:
    """Test for presence of levels in a factor

    Do any of `lvls` occur in `_f`?

    Args:
        _f: A factor
        lvls: A vector specifying levels to look for.

    Returns:
        A logical factor
    """
    _f = check_factor(_f)

    if is_scalar(lvls):
        lvls = [lvls]

    bad_lvls = setdiff(
        lvls,
        levels(_f, __calling_env=CallingEnvs.REGULAR),
        __calling_env=CallingEnvs.REGULAR,
    )
    if len(bad_lvls) > 0:
        bad_lvls = Array(bad_lvls)[~is_null(bad_lvls)]
    if len(bad_lvls) > 0:
        raise ValueError(f"Levels not present in factor: {bad_lvls}.")

    return numpy.isin(_f, lvls)


@register_verb(ForcatsRegType)
def fct_unique(_f: ForcatsType) -> Categorical:
    """Unique values of a factor

    Args:
        _f: A factor

    Returns:
        The factor with the unique values in `_f`
    """
    lvls = levels(_f, __calling_env=CallingEnvs.REGULAR)
    is_ord = is_ordered(_f, __calling_env=CallingEnvs.REGULAR)
    return factor(lvls, lvls, exclude=None, ordered=is_ord)
