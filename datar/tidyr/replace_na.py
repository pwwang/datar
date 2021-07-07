"""Replace NAs with specified values"""
from functools import singledispatch
from typing import Any, Iterable

import numpy
import pandas
from pandas import DataFrame
from pandas.core.series import Series
from pipda import register_verb

from ..core.types import ArrayLikeType, is_scalar
from ..core.contexts import Context


@singledispatch
def _replace_na(data: Iterable[Any], replace: Any) -> Iterable[Any]:
    """Replace NA for any iterables"""
    return type(data)( # type: ignore
        replace if is_scalar(elem) and pandas.isnull(elem) else elem
        for elem in data
    )


@_replace_na.register(numpy.ndarray)
@_replace_na.register(Series)
def _(data: ArrayLikeType, replace: Any) -> ArrayLikeType:
    """Replace NA for numpy.ndarray or Series"""
    ret = data.copy()
    ret[pandas.isnull(ret)] = replace
    return ret


@_replace_na.register(DataFrame)
def _(data: DataFrame, replace: Any) -> DataFrame:
    """Replace NA for numpy.ndarray or DataFrame"""
    # TODO: allow replace to be a list as an entire value to replace
    return data.fillna(replace)


@register_verb(
    (DataFrame, Series, numpy.ndarray, list, tuple, set), context=Context.EVAL
)
def replace_na(
    data: Iterable[Any],
    data_or_replace: Any = None,
    replace: Any = None,
) -> Any:
    """Replace NA with a value

    This function can be also used not as a verb. As a function called as
    an argument in a verb, data is passed implicitly. Then one could
    pass data_or_replace as the data to replace.

    Args:
        data: The data piped in
        data_or_replace: When called as argument of a verb, this is the
            data to replace. Otherwise this is the replacement.
        replace: The value to replace with
            Can only be a scalar or dict for data frame.
            So replace NA with a list is not supported yet.

    Returns:
        Corresponding data with NAs replaced
    """
    if data_or_replace is None and replace is None:
        return data.copy()

    if replace is None:
        # no replace, then data_or_replace should be replace
        replace = data_or_replace
    else:
        # replace specified, determine data
        # If data_or_replace is specified, it's data
        data = data if data_or_replace is None else data_or_replace

    return _replace_na(data, replace)
