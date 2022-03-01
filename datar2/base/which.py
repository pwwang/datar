"""Checking an iterable against itself or another one"""

import numpy as np
from ..core.factory import func_factory


which = func_factory(
    "transform",
    name="which",
    func=np.flatnonzero,
    doc="""Convert a bool iterable to indexes

    Args:
        x: An iterable of bools.
            Note that non-bool values will be converted into

    Returns:
        The indexes
    """,
)

which_min = func_factory(
    "agg",
    name="which_min",
    func=np.argmin,
    doc="""R's `which.min()`

    Get the index of the element with the maximum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the maximum value
    """,
)

which_max = func_factory(
    "agg",
    name="which_max",
    func=np.argmax,
    doc="""R's `which.max()`

    Get the index of the element with the minimum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the minimum value
    """,
)
