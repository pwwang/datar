"""Cumulative functions"""
import numpy as np
from pandas import Series
from pandas.core.generic import NDFrame
from pandas.core.groupby import GroupBy

from ..core.factory import func_factory

cumsum = func_factory(
    "transform",
    doc="""Cumulative sum of elements.

    Args:
        x: Input array

    Returns:
        An array of cumulative sum of elements in x
    """,
    func=np.cumsum,
)

cumprod = func_factory(
    "transform",
    doc="""Cumulative product of elements.

    Args:
        x: Input array

    Returns:
        An array of cumulative product of elements in x
    """,
    func=np.cumprod,
)


@func_factory("transform")
def cummin(x):
    """Cummulative min along elements in x

    Note that in `R`, it will be all NA's after an NA appears, but pandas will
    ignore that NA value

    Args:
        x: Input array

    Returns:
        An array of cumulative min of elements in x
    """
    return Series(x).cummin().values


cummin.register((NDFrame, GroupBy), "cummin")


@func_factory("transform")
def cummax(x):
    """Cummulative max along elements in x

    Note that in `R`, it will be all NA's after an NA appears, but pandas will
    ignore that NA value

    Args:
        x: Input array

    Returns:
        An array of cumulative max of elements in x
    """
    return Series(x).cummax().values


cummax.register((NDFrame, GroupBy), "cummax")
