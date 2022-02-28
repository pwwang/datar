"""Functions related to complex numbers"""
import numpy as np
from pandas.api.types import is_complex_dtype
from pipda import register_func

from ..core.contexts import Context
from ..core.factory import func_factory

from .testing import _register_type_testing
from .casting import _as_type


re = func_factory(
    "transform",
    name="re",
    doc="""Real part of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The real part of the complex numbers
    """,
    func=np.real,
)

im = func_factory(
    "transform",
    name="im",
    doc="""Real part of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The real part of the complex numbers
    """,
    func=np.imag,
)

mod = func_factory(
    "transform",
    name="mod",
    doc="""Modulus of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The Modulus of the complex numbers
    """,
    func=np.absolute,
)

arg = func_factory(
    "transform",
    name="arg",
    doc="""Angles of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The Angles of the complex numbers
    """,
    func=np.angle,
)

conj = func_factory(
    "transform",
    doc="""conjugate of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The conjugate of the complex numbers
    """,
    func=np.conj,
)


@register_func(None, context=Context.EVAL)
def as_complex(x, complex_type=np.complex_):
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
    scalar_types=(complex, np.complex_),
    dtype_checker=is_complex_dtype,
    doc="""Test if a value is complexes

    Args:
        x: The value to be checked

    Returns:
        True if the value is an complex or complexes; False otherwise.
    """,
)
