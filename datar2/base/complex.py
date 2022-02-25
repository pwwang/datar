"""Functions related to complex numbers"""
from typing import Any, Callable, Sequence

import numpy as np
from pandas import Series
from pandas.api.types import is_complex_dtype
from pandas.core.groupby import GroupBy
from pandas.core.generic import NDFrame
from pipda import register_func

from ..core.tibble import Tibble
from ..core.contexts import Context

from .testing import _register_type_testing
# from .casting import _as_type


def _ndframe_transform(x: NDFrame, fun: Callable) -> NDFrame:
    out = fun(x)
    if isinstance(x, Series):
        return Series(out, name=x.name, index=x.index)
    return Tibble(out, index=x.index)


@register_func(None, context=Context.EVAL)
def re(x) -> Sequence[complex]:
    """Real part of complex numbers

    Args:
        x: The complex numbers

    Returns:
        The real part of the complex numbers
    """
    if isinstance(x, GroupBy):
        return _ndframe_transform(x.obj, np.real)
    if isinstance(x, NDFrame):
        return _ndframe_transform(x, np.real)

    return np.real(x)


# im = register_numpy_func_x(
#     "im",
#     "imag",
#     doc="""Imaginary part of complex numbers

#     Args:
#         x: The complex numbers

#     Returns:
#         The imaginary part of the complex numbers
#     """,
# )

# mod = register_numpy_func_x(
#     "mod",
#     "absolute",
#     doc="""Modulus of complex numbers

#     Args:
#         x: The complex numbers

#     Returns:
#         The Modulus of the complex numbers
#     """,
# )

# arg = register_numpy_func_x(
#     "arg",
#     "angle",
#     doc="""Angles of complex numbers

#     Args:
#         x: The complex numbers

#     Returns:
#         The Angles of the complex numbers
#     """,
# )

# conj = register_numpy_func_x(
#     "conj",
#     "conj",
#     doc="""conjugate of complex numbers

#     Args:
#         x: The complex numbers

#     Returns:
#         The conjugate of the complex numbers
#     """,
# )


# @register_func(None, context=Context.EVAL)
# def as_complex(x: Any, complex_type=np.complex_) -> Sequence:
#     """Convert an object or elements of an iterable into complex

#     Args:
#         x: The object
#         complex_type: The type of the complex. Could be one of:
#             - numpy.complex128
#             - numpy.complex256
#             - numpy.complex64
#             - numpy.complex_

#     Returns:
#         When x is an array or a series, return x.astype(bool).
#         When x is iterable, convert elements of it into bools
#         Otherwise, convert x to bool.
#     """
#     return _as_type(x, complex_type)


# is_complex = _register_type_testing(
#     "is_complex",
#     scalar_types=(complex, np.complex_),
#     dtype_checker=is_complex_dtype,
#     doc="""Test if a value is complexes

#     Args:
#         x: The value to be checked

#     Returns:
#         True if the value is an complex or complexes; False otherwise.
#     """,
# )
