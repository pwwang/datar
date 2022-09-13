"""Testing functions in R"""

# all_equal, all_equal_numeric, ...
# all, any
# is_true, is_false
# is_finite, is_infinite, is_nan
# is_function, is_atomic, is_vector, is_array, is_character, is_complex,
# is_data_frame, is_double, is_element, (is_factor, is_ordered in base.factor)
# is_integer, is_list, is_logical, is_numeric
# is_unsorted, (is_qr in base.qr)
import builtins
from functools import singledispatch
from typing import Callable, Tuple, Type

import numpy as np
from pipda import register_func

from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import (
    is_scalar as is_scalar_pd,
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
)
from ..core.backends.pandas.core.groupby import SeriesGroupBy

from ..core.contexts import Context
from ..core.factory import func_factory

from .arithmetic import SINGLE_ARG_SIGNATURE


def _register_type_testing(
    name: str,
    scalar_types: Tuple[Type, ...],
    dtype_checker: Callable,
    doc: str = "",
):
    """Register type testing function"""
    @singledispatch
    def _dispatched(x):
        if is_scalar_pd(x):
            return isinstance(x, scalar_types)
        return builtins.all(isinstance(elem, scalar_types) for elem in x)

    @_dispatched.register(Series)
    @_dispatched.register(np.ndarray)
    def _(x):
        return dtype_checker(x)

    @_dispatched.register(SeriesGroupBy)
    def _(x):
        return x.agg(dtype_checker)

    @_dispatched.register(DataFrame)
    def _(x):
        return False

    @register_func
    def _testing(x):
        return _dispatched(x)

    _testing.__name__ = name
    _testing.__qualname__ = name
    _testing.__module__ = "datar.base"
    _testing.__doc__ = doc

    return _testing


is_numeric = _register_type_testing(
    "is_numeric",
    scalar_types=(int, float, complex, np.number),
    dtype_checker=is_numeric_dtype,
    doc="""Test if a value is numeric

    Args:
        x: The value to be checked

    Returns:
        True if the value is numeric; with a numeric dtype;
        or all elements are numeric
    """,
)

is_integer = _register_type_testing(
    "is_integer",
    scalar_types=(int, np.integer),
    dtype_checker=is_integer_dtype,
    doc="""Test if a value is integers

    Alias `is_int`

    Args:
        x: The value to be checked

    Returns:
        True if the value is an integer or integers; False otherwise.
    """,
)

is_int = is_integer

is_double = _register_type_testing(
    "is_double",
    scalar_types=(float, np.float_),
    dtype_checker=is_float_dtype,
    doc="""Test if a value is integers

    Alias `is_float`

    Args:
        x: The value to be checked

    Returns:
        True if the value is an integer or integers; False otherwise.
    """,
)

is_float = is_double


@register_func(None, context=Context.EVAL)
def is_atomic(x):
    """Check if x is an atomic or scalar value

    Args:
        x: The value to be checked

    Returns:
        True if x is atomic otherwise False
    """
    return is_scalar_pd(x)


is_scalar = is_atomic


@register_func(None, context=Context.EVAL)
def is_element(x, elems):
    """R's `is.element()` or `%in%`.
    Alias `is_in()`
    We can't do `a %in% b` in python (`in` behaves differently), so
    use this function instead
    """
    if isinstance(x, SeriesGroupBy) and isinstance(elems, SeriesGroupBy):
        from ..tibble import tibble

        df = tibble(x=x, y=elems)
        return df._datar["grouped"].apply(
            lambda g: np.isin(g.x, g.y)
        ).explode().astype(bool)

    if isinstance(x, SeriesGroupBy):
        out = x.transform(np.isin, test_elements=elems).groupby(
            x.grouper,
            observed=x.observed,
            sort=x.sort,
            dropna=x.dropna,
        )
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

    if isinstance(elems, SeriesGroupBy):
        return elems.apply(lambda e: np.isin(x, e)).explode().astype(bool)

    if isinstance(x, Series):
        return x.isin(elems)

    return np.isin(x, elems)


is_in = is_element


all = func_factory(
    kind="agg",
    func=builtins.all,
    doc="Check if all elements are true.",
    qualname="datar.base.all",
    signature=SINGLE_ARG_SIGNATURE,
)

any = func_factory(
    kind="agg",
    func=builtins.any,
    doc="Check if any elements is true.",
    qualname="datar.base.any",
    signature=SINGLE_ARG_SIGNATURE,
)
