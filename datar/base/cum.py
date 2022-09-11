"""Cumulative functions"""
import numpy as np

from ..core.backends.pandas.core.groupby import GroupBy

from ..core.tibble import TibbleGrouped
from ..core.factory import func_factory
from .arithmetic import SINGLE_ARG_SIGNATURE

cumsum = func_factory(
    kind="transform",
    doc="""Cumulative sum of elements.

    Args:
        x: Input array

    Returns:
        An array of cumulative sum of elements in x
    """,
    func=np.cumsum,
    signature=SINGLE_ARG_SIGNATURE,
)

cumprod = func_factory(
    kind="transform",
    doc="""Cumulative product of elements.

    Args:
        x: Input array

    Returns:
        An array of cumulative product of elements in x
    """,
    func=np.cumprod,
    signature=SINGLE_ARG_SIGNATURE,
)


@func_factory(kind="transform")
def cummin(x):
    """Cummulative min along elements in x

    Note that in `R`, it will be all NA's after an NA appears, but pandas will
    ignore that NA value

    Args:
        x: Input array

    Returns:
        An array of cumulative min of elements in x
    """
    return x.cummin()


# faster
cummin.register((TibbleGrouped, GroupBy), "cummin")


@func_factory(kind="transform")
def cummax(x):
    """Cummulative max along elements in x

    Note that in `R`, it will be all NA's after an NA appears, but pandas will
    ignore that NA value

    Args:
        x: Input array

    Returns:
        An array of cumulative max of elements in x
    """
    return x.cummax()


cummax.register((TibbleGrouped, GroupBy), "cummax")
