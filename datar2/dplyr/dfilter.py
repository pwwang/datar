"""Subset rows using column values

See source https://github.com/tidyverse/dplyr/blob/master/R/filter.R
"""
from typing import Iterable
import operator

import numpy as np
from pandas import DataFrame
from pandas.core.groupby import GroupBy
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import regcall, logger
from ..core.tibble import Tibble, TibbleGrouped, TibbleRowwise
from ..core.operator import _binop


@register_verb(DataFrame, context=Context.EVAL)
def filter(
    _data: DataFrame,
    *conditions: Iterable[bool],
    _preserve: bool = False,
) -> Tibble:
    """Subset a data frame, retaining all rows that satisfy your conditions

    Args:
        *conditions: Expressions that return logical values
        _preserve: Just for compatibility with `dplyr`'s `filter`.
            It's always `False` here.

    Returns:
        The subset dataframe
    """
    if _preserve:
        logger.warning("`filter()` doesn't support `_preserve` argument yet.")
    if _data.shape[0] == 0 or not conditions:
        return _data.copy()

    condition = np.array(True)
    for cond in conditions:
        condition = _binop(operator.and_, condition, cond)

    if isinstance(condition, GroupBy):
        condition = condition.obj

    if isinstance(condition, np.bool_):
        condition = bool(condition)

    if condition is True:
        out = _data.copy()
    elif condition is False:
        out = _data.take([])
    else:
        condition = condition.astype(bool)
        # Decouple Series/DataFrame
        condition = getattr(condition, "values", condition)
        out = _data[condition]

    if isinstance(_data, TibbleGrouped):
        out.reset_index(drop=True, inplace=True)
    return out


@filter.register(TibbleGrouped, context=Context.EVAL)
def _(
    _data: TibbleGrouped,
    *conditions: Iterable[bool],
    _preserve: bool = False,
) -> TibbleGrouped:
    """Filter on TibbleGrouped object"""
    out = regcall(
        filter,
        _data._datar["grouped"].obj,
        *conditions,
        _preserve=_preserve,
    )

    grouped = _data._datar["grouped"]
    return out.group_by(
        _data.group_vars,
        drop=grouped.observed,
        sort=grouped.sort,
        dropna=grouped.dropna,
    )


@filter.register(TibbleRowwise, context=Context.EVAL)
def _(
    _data: TibbleRowwise,
    *conditions: Iterable[bool],
    _preserve: bool = False,
) -> TibbleGrouped:
    """Filter on TibbleGrouped object"""
    out = regcall(
        filter,
        _data._datar["grouped"].obj,
        *conditions,
        _preserve=_preserve,
    )

    return out.rowwise(_data.group_vars)
