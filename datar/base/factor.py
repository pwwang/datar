"""Factors, implemented with pandas' Categorical

The huge difference:
R's factors support NAs in levels but Categorical cannot have NAs in categories.
"""
import numpy as np
from pipda import register_func

from ..core.backends.pandas import Categorical, Series
from ..core.backends.pandas.core.groupby import SeriesGroupBy
from ..core.backends.pandas.api.types import is_categorical_dtype, is_scalar

from ..core.contexts import Context


def _ensure_categorical(x):
    if is_categorical_dtype(x) and isinstance(x, Series):
        return x.values
    return x


@register_func(context=Context.EVAL)
def droplevels(x):
    """drop unused levels from a factor

    Args:
        x: The categorical data

    Returns:
        The categorical data with unused categories dropped.
    """
    return _ensure_categorical(x).remove_unused_categories()


@register_func(context=Context.EVAL)
def levels(x):
    """Get levels from a factor

    Args:
        x: The categorical data

    Returns:
        levels of the categorical
        None if x is not an categorical/factor
    """
    if not is_categorical_dtype(x):
        return None

    return _ensure_categorical(x).categories.values.copy()


@register_func(context=Context.EVAL)
def nlevels(x) -> int:
    """Get the number of levels of a factor

    Args:
        x: The data to get number of levels of

    Returns:
        Number of levels if x is a categorical/factor; otherwise 0
    """
    lvls = levels(x)
    return 0 if lvls is None else len(lvls)


@register_func(context=Context.EVAL)
def is_ordered(x) -> bool:
    """Check if a factor is ordered"""
    if not is_categorical_dtype(x):
        return False

    return _ensure_categorical(x).ordered


@register_func(context=Context.EVAL)
def factor(x=None, levels=None, exclude=np.nan, ordered=False):
    """encode a vector as a factor (the terms `category` and `enumerated type`
    are also used for factors).

    If argument ordered is TRUE, the factor levels are assumed to be ordered

    Args:
        x: a vector of data
        levels: an optional vector of the unique values (as character strings)
            that x might have taken.
        exclude: a vector of values to be excluded when forming the set of
            levels. This may be factor with the same level set as x or
            should be a character
        ordered: logical flag to determine if the levels should be regarded
            as ordered (in the order given).
    """
    if isinstance(x, SeriesGroupBy):
        out = factor.func(
            x.obj,
            levels=levels,
            exclude=exclude,
            ordered=ordered,
        )
        return Series(out, index=x.obj.index).groupby(
            x.grouper,
            observed=x.observed,
            sort=x.sort,
            dropna=x.dropna,
        )

    if x is None:
        x = []

    # pandas v1.3.0
    # FutureWarning: Allowing scalars in the Categorical constructor
    # is deprecated and will raise in a future version.
    if is_scalar(x):
        x = [x]

    if is_categorical_dtype(x):
        x = x.to_numpy()

    ret = Categorical(x, categories=levels, ordered=ordered)
    if exclude in [False, None]:
        return ret

    if is_scalar(exclude):
        exclude = [exclude]

    return ret.remove_categories(exclude)


def ordered(x=None, levels=None):
    """Create an ordered factor

    Args:
        x: The values to create factor

    Returns:
        The ordered factor
    """
    return factor(x, levels=levels).as_ordered()


@register_func(context=Context.EVAL)
def as_factor(x):
    """Convert an iterable into a pandas.Categorical object

    Args:
        x: The iterable

    Returns:
        The converted categorical object
    """
    return Categorical(x)


as_categorical = as_factor


@register_func(context=Context.EVAL)
def is_categorical(x):
    """Check if x is categorical data

    Alias `is_factor`

    Args:
        x: The value to be checked

    Returns:
        True if `x` is categorical else False
    """
    return is_categorical_dtype(x)


is_factor = is_categorical
