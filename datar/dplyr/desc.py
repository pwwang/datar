"""Provides desc"""
import numpy as np

from ..core.backends.pandas import Categorical, Series

from ..core.tibble import SeriesCategorical
from ..core.factory import func_factory


@func_factory(kind="transform")
def desc(x):
    """Transform a vector into a format that will be sorted in descending order

    This is useful within arrange().

    The original API:
    https://dplyr.tidyverse.org/reference/desc.html

    Args:
        x: vector to transform

    Returns:
        The descending order of x
    """
    try:
        out = -x
    except (ValueError, TypeError):
        cat = Categorical(x.values)
        out = desc.dispatch(SeriesCategorical)(
            Series(cat, index=x.index)
        )
    out.name = None
    return out


@desc.register(SeriesCategorical)
def _(x):
    cat = x.values
    code = cat.codes.astype(float)
    code[code == -1.0] = np.nan
    return Series(-code, index=x.index)
