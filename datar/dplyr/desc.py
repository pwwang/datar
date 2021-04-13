"""Provides desc"""
from typing import Any, Iterable
from pandas import Series, Categorical
from pipda import register_func

from ..core.contexts import Context
from ..base.constants import NA

@register_func(None, context=Context.EVAL)
def desc(x: Iterable[Any]) -> Series:
    """Transform a vector into a format that will be sorted in descending order

    This is useful within arrange().

    The original API:
    https://dplyr.tidyverse.org/reference/desc.html

    Args:
        x: vector to transform

    Returns:
        The descending order of x
    """
    x = Series(x)
    try:
        return -x
    except TypeError:
        cat = Categorical(x)
        code = Series(cat.codes).astype(float)
        code[code == -1.] = NA
        return -code
