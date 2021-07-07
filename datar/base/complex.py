"""Functions related to complex numbers"""
from typing import Any

import numpy
from pandas.core.dtypes.common import is_complex_dtype
from pipda import register_func

from ..core.utils import register_numpy_func_x
from ..core.types import ComplexOrIter
from ..core.contexts import Context

# pylint: disable=invalid-name
# pylint: disable=unused-import

from .testing import _register_type_testing
from .casting import _as_type

re = register_numpy_func_x(
    "re",
    "real",
    doc="""Real part of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The real part of the complex numbers
    """,
)

im = register_numpy_func_x(
    "im",
    "imag",
    doc="""Imaginary part of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The imaginary part of the complex numbers
    """,
)

mod = register_numpy_func_x(
    "mod",
    "absolute",
    doc="""Modulus of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The Modulus of the complex numbers
    """,
)

arg = register_numpy_func_x(
    "arg",
    "angle",
    doc="""Angles of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The Angles of the complex numbers
    """,
)

conj = register_numpy_func_x(
    "conj",
    "conj",
    doc="""conjugate of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The conjugate of the complex numbers
    """,
)


@register_func(None, context=Context.EVAL)
def as_complex(x: Any, complex_type=numpy.complex_) -> ComplexOrIter:
    """Convert an object or elements of an iterable into complex

    Args:
        x: The object
        complex_type: The type of the complex. Could be one of:
            - numpy.complex128
            - numpy.complex256
            - numpy.complex64
            - numpy.complex_

    Returns:
        When x is an array or a series, return x.astype(bool).
        When x is iterable, convert elements of it into bools
        Otherwise, convert x to bool.
    """
    return _as_type(x, complex_type)


is_complex = _register_type_testing(
    "is_complex",
    scalar_types=(complex, numpy.complex_),
    dtype_checker=is_complex_dtype,
    doc="""Test if a value is complexes

    Args:
        x: The value to be checked

    Returns:
        True if the value is an complex or complexes; False otherwise.
    """,
)
