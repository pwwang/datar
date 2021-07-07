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
from typing import Any, Callable, Iterable

import numpy
from pandas.core.dtypes.common import (
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
)
from pipda import register_func

from ..core.contexts import Context
from ..core.types import TypeOrIter, BoolOrIter, is_scalar

# pylint: disable=invalid-name


def _register_type_testing(
    name: str,
    scalar_types: TypeOrIter,
    dtype_checker: Callable[[Iterable], bool],
    doc: str = "",
) -> Callable:
    """Register type testing function"""

    @register_func(None, context=Context.EVAL)
    def _testing(x: Any) -> bool:
        """Type testing"""
        if is_scalar(x):
            return isinstance(x, scalar_types)
        if hasattr(x, "dtype"):
            return dtype_checker(x)
        return all(isinstance(elem, scalar_types) for elem in x)

    _testing.__name__ = name
    _testing.__doc__ = doc
    return _testing


is_numeric = _register_type_testing(
    "is_numeric",
    scalar_types=(int, float, complex, numpy.number),
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
    scalar_types=(int, numpy.integer),
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
    scalar_types=(float, numpy.float_),
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
def is_atomic(x: Any) -> bool:
    """Check if x is an atomic or scalar value

    Args:
        x: The value to be checked

    Returns:
        True if x is atomic otherwise False
    """
    return is_scalar(x)


@register_func(None)
def is_element(elem: Any, elems: Iterable[Any]) -> BoolOrIter:
    """R's `is.element()` or `%in%`.

    Alias `is_in()`

    We can't do `a %in% b` in python (`in` behaves differently), so
    use this function instead
    """
    out = numpy.isin(elem, elems)
    if is_scalar(elem):
        return bool(out)
    return out


is_in = is_element

# pylint: disable=unnecessary-lambda, redefined-builtin
all = register_func(None, context=Context.EVAL)(
    # can't set attributes to builtins.all, so wrap it.
    lambda arg: builtins.all(arg)
)
any = register_func(None, context=Context.EVAL)(
    lambda arg: builtins.any(arg)
)
