"""Checking an iterable against itself or another one"""

import numpy as np

from ..core.factory import func_factory

from .arithmetic import SINGLE_ARG_SIGNATURE

which = func_factory(
    kind="transform",
    name="which",
    qualname="datar.base.which",
    func=np.flatnonzero,
    doc="""Convert a bool iterable to indexes

    Args:
        x: An iterable of bools.
            Note that non-bool values will be converted into

    Returns:
        The indexes
    """,
    signature=SINGLE_ARG_SIGNATURE,
)

which_min = func_factory(
    kind="agg",
    name="which_min",
    qualname="datar.base.which_min",
    func=np.argmin,
    doc="""R's `which.min()`

    Get the index of the element with the maximum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the maximum value
    """,
    signature=SINGLE_ARG_SIGNATURE,
)

which_max = func_factory(
    kind="agg",
    name="which_max",
    qualname="datar.base.which_max",
    func=np.argmax,
    doc="""R's `which.max()`

    Get the index of the element with the minimum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the minimum value
    """,
    signature=SINGLE_ARG_SIGNATURE,
)
