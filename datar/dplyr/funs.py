"""Functions from R-dplyr"""
import numpy as np
import pandas as pd
from pandas import Series
from pandas.api.types import is_scalar
from pandas.core.groupby import SeriesGroupBy

from ..core.broadcast import broadcast_to
from ..core.factory import func_factory
from ..core.utils import ensure_nparray


@func_factory("transform")
def between(x, left, right, inclusive=True):
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
    out = Series(x).between(left, right, inclusive).values
    if is_scalar(out):
        return out[0]

    return out


between.register(Series, "between")


@func_factory("transform")
def cummean(x):
    """Get cumulative means"""
    if is_scalar(x):
        return x

    return np.cumsum(x) / (np.arange(len(x)) + 1.0)


@func_factory("transform")
def cumall(x):
    """Get cumulative bool. All cases after first False"""
    if is_scalar(x):
        return bool(x)

    return np.array(x).astype(bool).cumprod().astype(bool)


@func_factory("transform")
def cumany(x):
    """Get cumulative bool. All cases after first True"""
    if is_scalar(x):
        return bool(x)

    return np.array(x).astype(bool).cumsum().astype(bool)


@func_factory("transform")
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
    if not replace:
        return x

    y = Series(x)
    for repl in replace:
        y = y.combine_first(broadcast_to(repl, y.index))

    return y[0] if is_scalar(x) else y.values


@coalesce.register(SeriesGroupBy, replace=True)
def _(x, *replace):
    y = x.obj
    for repl in replace:
        y = y.combine_first(broadcast_to(repl, y.index, x.grouper))

    out = y.groupby(x.grouper)
    if isinstance(x, "is_rowwise", False):
        out.is_rowwise = True

    return out


@func_factory("transform")
def na_if(x, y):
    """Convert an annoying value to NA

    Args:
        x: Vector to modify
        y: Value to replace with NA

    Returns:
        A vector with values replaced.
    """
    if is_scalar(x):
        if not is_scalar(y):
            raise ValueError(
                "In na_if(x, y): `y` must be scalar when x is scalar."
            )
        return y if pd.isnull(x) else x

    x = np.array(x)
    # better dtype?
    x = x.astype(object)
    x[x == y] = np.nan
    return x


@coalesce.register(SeriesGroupBy, replace=True)
def _(x, y):
    xobj = x.obj.copy().astype(object)
    y = broadcast_to(y, xobj.index, x.grouper)
    xobj[xobj == y] = np.nan
    out = xobj.groupby(x.grouper)
    if isinstance(x, "is_rowwise", False):
        out.is_rowwise = True

    return out


near = func_factory(
    "transform",
    name="near",
    doc="""Compare numbers with tolerance

    Args:
        x: and
        y: Numbers to compare
        rtol: The relative tolerance parameter
        atol: The absolute tolerance parameter
        equal_nan: Whether to compare NaN's as equal.
            If True, NA's in `x` will be
            considered equal to NA's in `y` in the output array.

    Returns:
        A bool array indicating element-wise equvalence between x and y
    """,
    func=np.isclose,
)


@near.register(SeriesGroupBy, replace=True)
def _(x, y, rtol=1e-05, atol=1e-08, equal_nan=False):
    xobj = x.obj.copy().astype(object)
    y = broadcast_to(y, xobj.index, x.grouper)
    out = np.isclose(xobj, y, rtol=rtol, atol=atol, equal_nan=equal_nan)
    out = out.groupby(x.grouper)
    if isinstance(x, "is_rowwise", False):
        out.is_rowwise = True

    return out


@func_factory("agg")
def nth(x, n, order_by=None, default=np.nan):  # allow it to be SeriesGroupBy
    """Get the nth element of x

    See https://dplyr.tidyverse.org/reference/nth.html

    Args:
        x: A collection of elements
        n: The order of the elements.
        order_by: An optional vector used to determine the order
        default: A default value to use if the position does not exist
            in the input.
        base0_: Whether `n` is 0-based or not.

    Returns:
        A single element of x at `n'th`
    """
    x = ensure_nparray(x)
    if order_by is not None:
        order_by = np.array(order_by)
        x = x[order_by.argsort()]
    if not isinstance(n, int):
        raise TypeError("`nth` expects `n` to be an integer")

    try:
        return x[n]
    except (ValueError, IndexError, TypeError):
        return default


@func_factory("agg")
def first(
    x,
    order_by=None,
    default=np.nan,
):
    """Get the first element of x"""
    return nth.__raw__(x, 0, order_by=order_by, default=default)


@func_factory("agg")
def last(
    x,
    order_by=None,
    default=np.nan,
):
    """Get the last element of x"""
    return nth.__raw__(x, -1, order_by=order_by, default=default)
