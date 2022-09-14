"""Functions related to complex numbers"""
import numpy as np
from pipda import register_func

from ..core.backends.pandas.api.types import is_complex_dtype

from ..core.contexts import Context
from ..core.factory import func_factory

from .testing import _register_type_testing
from .casting import _as_type
from .arithmetic import SINGLE_ARG_SIGNATURE

re = func_factory(
    kind="transform",
    name="re",
    doc="""Real part of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The real part of the complex numbers
    """,
    func=np.real,
    signature=SINGLE_ARG_SIGNATURE,
)

im = func_factory(
    kind="transform",
    name="im",
    doc="""Real part of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The real part of the complex numbers
    """,
    func=np.imag,
    signature=SINGLE_ARG_SIGNATURE,
)

mod = func_factory(
    kind="transform",
    name="mod",
    qualname="mod",
    module="datar.base",
    doc="""Modulus of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The Modulus of the complex numbers
    """,
    func=np.absolute,
    signature=SINGLE_ARG_SIGNATURE,
)

arg = func_factory(
    kind="transform",
    name="arg",
    doc="""Angles of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The Angles of the complex numbers
    """,
    func=np.angle,
    signature=SINGLE_ARG_SIGNATURE,
)

conj = func_factory(
    kind="transform",
    qualname="conj",
    module="datar.base",
    doc="""conjugate of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The conjugate of the complex numbers
    """,
    func=np.conj,
    signature=SINGLE_ARG_SIGNATURE,
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
