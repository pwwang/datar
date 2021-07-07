"""Factors, implemented with pandas' Categorical

The huge difference:
R's factors support NAs in levels but Categorical cannot have NAs in categories.
"""

from typing import Any, Iterable

from pandas import Categorical
from pipda import register_func

from ..core.contexts import Context
from ..core.types import (
    is_scalar,
    is_categorical as is_categorical_,
    ArrayLikeType,
)
from ..core.utils import categorized

from .na import NA

# factor, ordered, is_factor, is_ordered, as_factor, as_ordered, add_na

# pylint: disable=invalid-name


@register_func(None, context=Context.EVAL)
def droplevels(x: Categorical) -> Categorical:
    """drop unused levels from a factor

    Args:
        x: The categorical data

    Returns:
        The categorical data with unused categories dropped.
    """
    return categorized(x).remove_unused_categories()


@register_func(None, context=Context.EVAL)
def levels(x: Any) -> ArrayLikeType:
    """Get levels from a factor

    Args:
        x: The categorical data

    Returns:
        levels of the categorical data
    """
    if not is_categorical_(x):
        return None

    return categorized(x).categories


def factor(
    x: Iterable[Any] = None,
    # pylint: disable=redefined-outer-name
    levels: Iterable[Any] = None,
    exclude: Any = NA,
    ordered: bool = False,
) -> Categorical:
    """encode a vector as a factor (the terms ‘category’ and ‘enumerated type’
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
    if x is None:
        x = []

    # pandas v1.3.0
    # FutureWarning: Allowing scalars in the Categorical constructor
    # is deprecated and will raise in a future version.
    if is_scalar(x):
        x = [x]

    if is_categorical_(x):
        x = x.to_numpy()

    ret = Categorical(x, categories=levels, ordered=ordered)
    if is_scalar(exclude):
        exclude = [exclude]

    return ret.remove_categories(exclude)


@register_func(None, context=Context.EVAL)
def as_factor(x: Iterable) -> Categorical:
    """Convert an iterable into a pandas.Categorical object

    Args:
        x: The iterable

    Returns:
        The converted categorical object
    """
    return Categorical(x)


as_categorical = as_factor


@register_func(None, context=Context.EVAL)
def is_categorical(x: Any) -> bool:
    """Check if x is categorical data

    Alias `is_factor`

    Args:
        x: The value to be checked

    Returns:
        True if `x` is categorical else False
    """
    return is_categorical_(x)


is_factor = is_categorical
