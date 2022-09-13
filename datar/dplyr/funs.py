"""Functions from R-dplyr"""
import numpy as np

from datar.core.tibble import TibbleGrouped, TibbleRowwise

from ..core.backends import pandas as pd
from ..core.backends.pandas import Series
from ..core.backends.pandas.api.types import is_scalar
from ..core.backends.pandas.core.groupby import SeriesGroupBy

from ..core.factory import func_factory


@func_factory(kind="transform")
def between(x, left, right, inclusive="both"):
    """Function version of `left <= x <= right`, works for both scalar and
    vector data

    See https://dplyr.tidyverse.org/reference/between.html

    Args:
        x: The data to test
        left: and
        right: The boundary values (must be scalars)
        inclusive: Either `both`, `neither`, `left` or `right`.
            Include boundaries. Whether to set each bound as closed or open.

    Returns:
        A bool value if `x` is scalar, otherwise an array of boolean values
        Note that it will be always False when NA appears in x, left or right.
    """
    return x.between(left, right, inclusive)


# faster
between.register(SeriesGroupBy, "between")


@func_factory(kind="transform")
def cummean(x: Series):
    """Get cumulative means"""
    return x.cumsum() / (np.arange(x.size) + 1.0)


@func_factory(kind="transform")
def cumall(x, na_as=False):
    """Get cumulative bool. All cases after first False"""
    return x.fillna(na_as).cumprod().astype(bool)


@func_factory(kind="transform")
def cumany(x, na_as=False):
    """Get cumulative bool. All cases after first True"""
    return x.fillna(na_as).cumsum().astype(bool)


@func_factory({"x", "replace"}, "transform")
def coalesce(x, *replace):
    """Replace missing values

    https://dplyr.tidyverse.org/reference/coalesce.html

    Args:
        x: The vector to replace
        replace: The replacement

    Returns:
        A vector the same length as the first argument with missing values
        replaced by the first non-missing value.
    """
    for repl in replace:
        x = x.combine_first(repl)

    return x


@func_factory({"x", "y"})
def na_if(x, y, __args_raw=None):
    """Convert an annoying value to NA

    Args:
        x: Vector to modify
        y: Value to replace with NA

    Returns:
        A vector with values replaced.
    """
    if is_scalar(x) and is_scalar(y):  # rowwise
        return np.nan if x == y else x

    rawx = __args_raw["x"] if __args_raw else x
    lenx = 1 if is_scalar(rawx) else len(rawx)
    if lenx < y.size:
        raise ValueError(
            f"`y` must be length {lenx} (same as `x`), not {y.size}."
        )
    x[(x == y).values] = np.nan
    return x


@na_if.register((TibbleGrouped, TibbleRowwise), func="default", post="decor")
def _na_if_post(__out, x, y, __args_raw=None):
    rawx = __args_raw["x"]
    out = __out.groupby(
        rawx.grouper,
        sort=rawx.sort,
        dropna=rawx.dropna,
        observed=rawx.observed,
    )
    if getattr(rawx, "is_rowwise", False):
        out.is_rowwise = True
    return out


near = func_factory(
    {"a", "b"},
    name="near",
    qualname="near",
    module="datar.dplyr",
    doc="""Compare numbers with tolerance

    Args:
        a: and
        b: Numbers to compare
        rtol: The relative tolerance parameter
        atol: The absolute tolerance parameter
        equal_nan: Whether to compare NaN's as equal.
            If True, NA's in `a` will be
            considered equal to NA's in `b` in the output array.

    Returns:
        A bool array indicating element-wise equvalence between a and b
    """,
    func=lambda a, b, __args_raw=None: np.isclose(a, b),
)


@near.register((TibbleGrouped, TibbleRowwise), func="default", post="decor")
def _near_post(__out, a, b, __args_raw=None):
    rawx = __args_raw["a"]
    out = __out.groupby(
        rawx.grouper,
        sort=rawx.sort,
        dropna=rawx.dropna,
        observed=rawx.observed,
    )
    if getattr(rawx, "is_rowwise", False):
        out.is_rowwise = True
    return out


def _nth(x, n, order_by=np.nan, default=np.nan, __args_raw=None):
    if not isinstance(n, int):
        raise TypeError("`nth` expects `n` to be an integer")

    order_by_null = pd.isnull(__args_raw["order_by"])
    if is_scalar(order_by_null):
        order_by_null = np.array([order_by_null], dtype=bool)

    if not order_by_null.all():
        x = x.iloc[order_by.argsort().values]

    try:
        return x.iloc[n]
    except (ValueError, IndexError, TypeError):
        return default


@func_factory({"x", "order_by"}, "agg")
def nth(x, n, order_by=np.nan, default=np.nan, __args_raw=None):
    """Get the nth element of x

    See https://dplyr.tidyverse.org/reference/nth.html

    Args:
        x: A collection of elements
        n: The order of the elements.
        order_by: An optional vector used to determine the order
        default: A default value to use if the position does not exist
            in the input.

    Returns:
        A single element of x at `n'th`
    """
    return _nth(
        x, n, order_by=order_by, default=default, __args_raw=__args_raw
    )


@func_factory({"x", "order_by"}, "agg")
def first(
    x,
    order_by=np.nan,
    default=np.nan,
    __args_raw=None,
):
    """Get the first element of x"""
    return _nth(
        x, 0, order_by=order_by, default=default, __args_raw=__args_raw
    )


@func_factory({"x", "order_by"}, "agg")
def last(
    x,
    order_by=np.nan,
    default=np.nan,
    __args_raw=None,
):
    """Get the last element of x"""
    return _nth(
        x, -1, order_by=order_by, default=default, __args_raw=__args_raw
    )
