"""Checking an iterable against itself or another one"""
from typing import Iterable

import numpy
from pipda import register_func

from ..core.utils import get_option
from ..core.contexts import Context


@register_func(None, context=Context.EVAL)
def which(x: Iterable[bool], base0_: bool = None) -> Iterable[int]:
    """Convert a bool iterable to indexes

    Args:
        x: An iterable of bools.
            Note that non-bool values will be converted into
        base0_: Whether the returned indexes are 0-based.
            Controlled by `get_option('which.base.0')` if not provided

    Returns:
        The indexes
    """
    return numpy.flatnonzero(x) + int(not get_option("which.base.0", base0_))


@register_func(None)
def which_min(x: Iterable, base0_: bool = None) -> int:
    """R's `which.min()`

    Get the index of the element with the maximum value

    Args:
        x: The iterable
        base0_: Whether the index to return is 0-based or not.
            Controlled by `get_option('which.base.0')` if not provided

    Returns:
        The index of the element with the maximum value
    """
    return numpy.argmin(x) + int(not get_option("which.base.0", base0_))


@register_func(None)
def which_max(x: Iterable, base0_: bool = True) -> int:
    """R's `which.max()`

    Get the index of the element with the minimum value

    Args:
        x: The iterable
        base0_: Whether the index to return is 0-based or not
            Not that this is not controlled by `get_option('index.base.0')`

    Returns:
        The index of the element with the minimum value
    """
    return numpy.argmax(x) + int(not get_option("which.base.0", base0_))
