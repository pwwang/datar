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
from ..core.broadcast import broadcast_to
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

    grouper = None
    if isinstance(_data, TibbleGrouped):
        grouper = _data._datar["grouped"].grouper

    condition = broadcast_to(condition, _data.index, grouper)
    if isinstance(condition, np.bool_):
        condition = bool(condition)

    if condition is True:
        return _data.copy()
    if condition is False:
        return _data.take([])

    condition = getattr(condition, "values", condition)
    return _data[condition]
