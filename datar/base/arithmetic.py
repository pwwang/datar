"""Arithmetic or math functions"""

from multiprocessing.dummy import Array
from typing import Any, Callable, Iterable, Optional

import numpy
from pandas import DataFrame, Series
from pipda import register_func, register_verb

from ..core.contexts import Context
from ..core.types import NumericOrIter, NumericType, is_not_null
from ..core.utils import Array, register_numpy_func_x, recycle_value, length_of
from ..core.collections import Collection

# TODO: docstring
# weighted_mean, sd, cor?, range, quantile, summary, iqr

def _register_arithmetic_agg(
        name: str,
        np_name: str,
        doc: str = ""
) -> Callable:
    """Register an arithmetic function"""
    @register_func(None, context=Context.EVAL)
    def _arithmetric(x: Iterable, na_rm: bool = False):
        """Arithmetric function"""
        if na_rm:
            x = Array(x)[is_not_null(x)]
        return getattr(numpy, np_name)(x)

    _arithmetric.__name__ = name
    _arithmetric.__doc__ = doc
    return _arithmetric

# pylint: disable=invalid-name
# pylint: disable=redefined-builtin
sum = _register_arithmetic_agg(
    'sum',
    'sum',
    doc="""Sum of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The sum of the input
    """
)

mean = _register_arithmetic_agg(
    'mean',
    'mean',
    doc="""Mean of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The mean of the input
    """
)

median = _register_arithmetic_agg(
    'median',
    'median',
    doc="""Median of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The median of the input
    """
)

@register_func(None, context=Context.EVAL)
def min(*x, na_rm: bool = False) -> Any:
    """Min of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The min of the input
    """
    fun = numpy.nanmin if na_rm else numpy.min
    x = Collection(*x) # flatten
    return fun(x)


@register_func(None, context=Context.EVAL)
def max(*x, na_rm: bool = False) -> Any:
    """Max of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The max of the input
    """
    fun = numpy.nanmax if na_rm else numpy.max
    x = Collection(*x) # flatten
    return fun(x)

@register_func(None, context=Context.EVAL)
def var(x: Any, na_rm: bool = False, ddof: int = 1):
    """Variance of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs
        ddof: Delta Degrees of Freedom

    Returns:
        The variance of the input
    """
    fun = numpy.nanvar if na_rm else numpy.var
    return fun(x, ddof=ddof)


@register_func(None, context=Context.EVAL)
def pmin(
        *x: NumericType,
        na_rm: bool = False
) -> Iterable[float]:
    """Get the min value rowwisely

    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise min of `*x`
    """
    maxlen = max(map(length_of, x))
    x = (recycle_value(elem, maxlen) for elem in x)
    return Array([min(elem, na_rm=na_rm) for elem in zip(*x)])

@register_func(None, context=Context.EVAL)
def pmax(
        *x: Iterable,
        na_rm: bool = False
) -> Iterable[float]:
    """Get the max value rowwisely
    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise max of `*x`
    """
    maxlen = max(map(length_of, x))
    x = (recycle_value(elem, maxlen) for elem in x)
    return Array([max(elem, na_rm=na_rm) for elem in zip(*x)])

@register_func(None, context=Context.EVAL)
def round(
        x: NumericOrIter,
        ndigits: int = 0
) -> NumericOrIter:
    """Rounding a number"""
    return numpy.round(x, ndigits)

sqrt = register_numpy_func_x(
    'sqrt',
    'sqrt',
    doc="""Get the square root of a number/numbers

    Args:
        x: The input

    Returns:
        The square root of the input
    """
)

abs = register_numpy_func_x(
    'abs',
    'abs',
    doc="""Get the absolute value of a number/numbers

    Args:
        x: The input

    Returns:
        The absolute values of the input
    """
)

ceiling = register_numpy_func_x(
    'ceiling',
    'ceil',
    doc="""Get the ceiling integer of a number/numbers

    Args:
        x: The input

    Returns:
        The ceiling integer of the input
    """
)

floor = register_numpy_func_x(
    'floor',
    'floor',
    doc="""Get the floor integer of a number/numbers

    Args:
        x: The input

    Returns:
        The floor integer of the input
    """
)

# pylint: disable=unused-argument
@register_verb(DataFrame, context=Context.EVAL)
def cov(x: DataFrame, y: Optional[Iterable] = None, ddof: int = 1) -> DataFrame:
    """Compute pairwise covariance of dataframe columns,
    or between two variables
    """
    # TODO: support na_rm, use, method. see `?cov` in R
    return x.cov(ddof=ddof)

@cov.register((numpy.ndarray, Series, list, tuple), context=Context.EVAL)
def _(x: Iterable, y: Iterable, ddof: int = 1) -> DataFrame:
    """Compute covariance for two iterables"""
    # ddof: numpy v1.5+
    return numpy.cov(x, y, ddof=ddof)[0][1]
