"""Checking an iterable against itself or another one"""
from typing import Iterable, Optional

import numpy
from pipda import register_func

from ..core.utils import get_option
from ..core.contexts import Context

@register_func(None, context=Context.EVAL)
def which(x: Iterable[bool], _base0: Optional[bool] = None) -> Iterable[int]:
    """Convert a bool iterable to indexes

    Args:
        x: An iterable of bools.
            Note that non-bool values will be converted into
        _base0: Whether the returned indexes are 0-based.
            Controlled by `get_option('which.base.0')` if not provided

    Returns:
        The indexes
    """
    return numpy.flatnonzero(x) + int(not get_option('which.base.0', _base0))

@register_func(None)
def which_min(x: Iterable, _base0: Optional[bool] = None) -> int:
    """R's `which.min()`

    Get the index of the element with the maximum value

    Args:
        x: The iterable
        _base0: Whether the index to return is 0-based or not.
            Controlled by `get_option('which.base.0')` if not provided

    Returns:
        The index of the element with the maximum value
    """
    return numpy.argmin(x) + int(not get_option('which.base.0', _base0))

@register_func(None)
def which_max(x: Iterable, _base0: bool = True) -> int:
    """R's `which.max()`

    Get the index of the element with the minimum value

    Args:
        x: The iterable
        _base0: Whether the index to return is 0-based or not
            Not that this is not controlled by `get_option('index.base.0')`

    Returns:
        The index of the element with the minimum value
    """
    return numpy.argmax(x) + int(not get_option('which.base.0', _base0))
