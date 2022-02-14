"""Checking an iterable against itself or another one"""
from typing import Iterable

import numpy
from pipda import register_func

from ..core.contexts import Context


@register_func(None, context=Context.EVAL)
def which(x: Iterable[bool]) -> Iterable[int]:
    """Convert a bool iterable to indexes

    Args:
        x: An iterable of bools.
            Note that non-bool values will be converted into

    Returns:
        The indexes
    """
    return numpy.flatnonzero(x)


@register_func(None)
def which_min(x: Iterable) -> int:
    """R's `which.min()`

    Get the index of the element with the maximum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the maximum value
    """
    return numpy.argmin(x)


@register_func(None)
def which_max(x: Iterable) -> int:
    """R's `which.max()`

    Get the index of the element with the minimum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the minimum value
    """
    return numpy.argmax(x)
