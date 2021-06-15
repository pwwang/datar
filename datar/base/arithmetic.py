"""Arithmetic functions"""

from multiprocessing.dummy import Array
from typing import Callable, Iterable

import numpy
from pipda import register_func

from ..core.contexts import Context
from ..core.types import NumericType, is_not_null
from ..core.utils import Array

# TODO: docstring

def _register_arithmetic(name: str, doc: str = "") -> Callable:
    """Register an arithmetic function"""
    @register_func(None, context=Context.EVAL)
    def _arithmetric(x: Iterable, na_rm: bool = False):
        """Arithmetric function"""
        if na_rm:
            x = Array(x)[is_not_null(x)]
        return getattr(numpy, name)(x)

    _arithmetric.__name__ = name
    _arithmetric.__doc__ = doc
    return _arithmetric

# pylint: disable=invalid-name
# pylint: disable=redefined-builtin
sum = _register_arithmetic('sum', "Sum of an iterable.")
mean = _register_arithmetic('mean', 'Calculate mean of an iterable.')
median = _register_arithmetic('median', 'Calculate median of an iterable.')
min = _register_arithmetic('min', 'Get min of an iterable.')
max = _register_arithmetic('min', 'Get max of an iterable.')


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
