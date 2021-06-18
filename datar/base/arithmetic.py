"""Arithmetic or math functions"""

from multiprocessing.dummy import Array
from typing import Callable, Iterable

import numpy
from pipda import register_func

from ..core.contexts import Context
from ..core.types import NumericOrIter, NumericType, is_not_null
from ..core.utils import Array, register_numpy_func_x

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

min = _register_arithmetic_agg(
    'min',
    'min',
    doc="""Min of the input.

Args:
    x: The input
    na_rm: Exclude the NAs

Returns:
    The min of the input
"""
)

max = _register_arithmetic_agg(
    'max',
    'max',
    doc="""Max of the input.

Args:
    x: The input
    na_rm: Exclude the NAs

Returns:
    The max of the input
"""
)

var = _register_arithmetic_agg(
    'var',
    'var',
    doc="""Variance of the input.

Args:
    x: The input
    na_rm: Exclude the NAs

Returns:
    The variance of the input
"""
)

@register_func(None, context=Context.EVAL)
def pmin(
        *x: NumericType,
        na_rm: bool = False
) -> Iterable[float]:
    """Get the min value rowwisely"""
    return Array([min(elem, na_rm=na_rm) for elem in zip(*x)])

@register_func(None, context=Context.EVAL)
def pmax(
        *x: Iterable,
        na_rm: bool = False
) -> Iterable[float]:
    """Get the max value rowwisely"""
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
