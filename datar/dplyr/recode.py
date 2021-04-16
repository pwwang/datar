"""Recode values

https://github.com/tidyverse/dplyr/blob/master/R/recode.R
"""
# TODO: add tests
from typing import Any, Iterable

import numpy
import pandas
from pandas import Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from pipda import register_func

from ..core.contexts import Context
from ..base.constants import NA

@register_func(None, context=Context.EVAL)
def recode(
        series: Iterable[Any],
        *args: Any,
        _default: Any = None,
        _missing: Any = NA,
        **kwargs: Any
) -> Iterable[Any]:
    """Recode a vector, replacing elements in it

    Args:
        series: A vector to modify
        *args, **kwargs: replacements
        _default: If supplied, all values not otherwise matched will be
            given this value. If not supplied and if the replacements are
            the same type as the original values in series, unmatched values
            are not changed. If not supplied and if the replacements are
            not compatible, unmatched values are replaced with NA.
        _missing: If supplied, any missing values in .x will be replaced
            by this value.

    Returns:
        The vector with values replaced
    """
    kwd_recodes = {}
    for i, arg in enumerate(args):
        if isinstance(arg, dict):
            kwd_recodes.update(arg)
        else:
            kwd_recodes[i] = arg

    kwd_recodes.update(kwargs)

    series = numpy.array(series) # copied
    ret = [_missing] * len(series)

    for elem in set(series):
        if pandas.isna(elem):
            continue
        replace = kwd_recodes.get(elem, _default)
        replace = elem if replace is None else replace

        for i, indicator in enumerate(series == elem):
            if not indicator:
                continue
            ret[i] = replace

    return ret

@register_func(None, context=Context.EVAL)
def recode_factor(
        series: Iterable[Any],
        *args: Any,
        _default: Any = None,
        _missing: Any = NA,
        _ordered: bool = False,
        **kwargs: Any
) -> Iterable[Any]:
    """Recode a factor

    see recode().
    """
    if not is_categorical_dtype(series):
        series = Categorical(series)
    else:
        _default = NA if _default is None else _default

    categories = recode(
        series,
        *args,
        _default=_default,
        _missing=_missing,
        **kwargs
    )
    cats = []
    for cat in categories:
        if pandas.isnull(cat):
            continue
        if cat not in cats:
            cats.append(cat)

    series = recode(
        series,
        *args,
        _default=_default,
        _missing=_missing,
        **kwargs
    )

    return Categorical(
        series,
        categories=cats,
        ordered=_ordered
    )

recode_categorical = recode_factor # pylint: disable=invalid-name
