"""Provides other helper functions for factors"""
from typing import Any, Iterable

import numpy as np
from ..core.backends import pandas as pd
from ..core.backends.pandas import Categorical, DataFrame
from ..core.backends.pandas.api.types import is_scalar
from pipda import register_verb

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

from .utils import check_factor, ForcatsRegType
from .lvl_order import fct_inorder


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_count(_f, sort: bool = False, prop=False) -> Categorical:
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
    n_na = sum(pd.isnull(f2))

    df = DataFrame(
        {
            "f": fct_inorder(levels(f2), __ast_fallback="normal"),
            "n": tabulate(f2, nlevels(f2)),
        }
    )

    if n_na > 0:
        df.loc[df.shape[0], :] = {"f": NA, "n": n_na}

    if sort:
        df = arrange(
            df,
            desc(f.n),
            __ast_fallback="normal",
        )
    if prop:
        df = mutate(
            df,
            p=prop_table(f.n, __ast_fallback="normal"),
            __ast_fallback="normal",
        )

    return df


@register_verb(ForcatsRegType, context=Context.EVAL)
def fct_match(_f, lvls: Any) -> Iterable[bool]:
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

    bad_lvls = setdiff(lvls, levels(_f), __ast_fallback="normal")
    if len(bad_lvls) > 0:
        bad_lvls = np.array(bad_lvls)[~pd.isnull(bad_lvls)]
    if len(bad_lvls) > 0:
        raise ValueError(f"Levels not present in factor: {bad_lvls}.")

    return np.isin(_f, lvls)


@register_verb(ForcatsRegType)
def fct_unique(_f) -> Categorical:
    """Unique values of a factor

    Args:
        _f: A factor

    Returns:
        The factor with the unique values in `_f`
    """
    lvls = levels(_f)
    is_ord = is_ordered(_f)
    return factor(lvls, lvls, exclude=None, ordered=is_ord)
