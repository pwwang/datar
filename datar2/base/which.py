"""Checking an iterable against itself or another one"""

from ..core.utils import transform_func


which = transform_func(
    "which",
    transform="flatnonzero",
    doc="""Convert a bool iterable to indexes

    Args:
        x: An iterable of bools.
            Note that non-bool values will be converted into

    Returns:
        The indexes
    """,
)

which_min = transform_func(
    "which_min",
    transform="argmin",
    doc="""R's `which.min()`

    Get the index of the element with the maximum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the maximum value
    """,
)

which_max = transform_func(
    "which_max",
    transform="argmax",
    doc="""R's `which.max()`

    Get the index of the element with the minimum value

    Args:
        x: The iterable

    Returns:
        The index of the element with the minimum value
    """,
)
