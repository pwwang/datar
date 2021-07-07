"""Trigonometric and Hyperbolic Functions"""

from typing import Callable

import numpy
from pipda import register_func

from ..core.contexts import Context
from ..core.types import FloatOrIter
from .constants import pi

# pylint: disable=invalid-name


def _register_trig_hb_func(name: str, np_name: str, doc: str) -> Callable:
    """Register trigonometric and hyperbolic function"""
    np_fun = getattr(numpy, np_name)
    if name.endswith("pi"):
        func = lambda x: np_fun(x * pi)
    else:
        # ufunc cannot set context
        func = lambda x: np_fun(x)  # pylint: disable=unnecessary-lambda

    func = register_func(None, context=Context.EVAL, func=func)
    func.__name__ = name
    func.__doc__ = doc
    return func


sin = _register_trig_hb_func(
    "sin",
    "sin",
    doc="""The sine function

Args:
    x: a numeric value or iterable

Returns:
    The sine value of `x`
""",
)

cos = _register_trig_hb_func(
    "cos",
    "cos",
    doc="""The cosine function

Args:
    x: a numeric value or iterable

Returns:
    The cosine value of `x`
""",
)

tan = _register_trig_hb_func(
    "tan",
    "tan",
    doc="""The tangent function

Args:
    x: a numeric value or iterable

Returns:
    The tangent value of `x`
""",
)

acos = _register_trig_hb_func(
    "acos",
    "arccos",
    doc="""The arc-cosine function

Args:
    x: a numeric value or iterable

Returns:
    The arc-cosine value of `x`
""",
)

asin = _register_trig_hb_func(
    "acos",
    "arcsin",
    doc="""The arc-sine function

Args:
    x: a numeric value or iterable

Returns:
    The arc-sine value of `x`
""",
)

atan = _register_trig_hb_func(
    "acos",
    "arctan",
    doc="""The arc-sine function

Args:
    x: a numeric value or iterable

Returns:
    The arc-sine value of `x`
""",
)

sinpi = _register_trig_hb_func(
    "sinpi",
    "sin",
    doc="""The sine function

Args:
    x: a numeric value or iterable, which is the multiple of pi

Returns:
    The sine value of `x`
""",
)

cospi = _register_trig_hb_func(
    "cospi",
    "cos",
    doc="""The cosine function

Args:
    x: a numeric value or iterable, which is the multiple of pi

Returns:
    The cosine value of `x`
""",
)

tanpi = _register_trig_hb_func(
    "tanpi",
    "tan",
    doc="""The tangent function

Args:
    x: a numeric value or iterable, which is the multiple of pi

Returns:
    The tangent value of `x`
""",
)

cosh = _register_trig_hb_func(
    "cosh",
    "cosh",
    doc="""Hyperbolic cosine

Args:
    x: a numeric value or iterable

Returns:
    The hyperbolic cosine value of `x`
""",
)

sinh = _register_trig_hb_func(
    "sinh",
    "sinh",
    doc="""Hyperbolic sine

Args:
    x: a numeric value or iterable

Returns:
    The hyperbolic sine value of `x`
""",
)

tanh = _register_trig_hb_func(
    "tanh",
    "tanh",
    doc="""Hyperbolic tangent

Args:
    x: a numeric value or iterable

Returns:
    The hyperbolic tangent value of `x`
""",
)

acosh = _register_trig_hb_func(
    "acosh",
    "arccosh",
    doc="""Hyperbolic arc-cosine

Args:
    x: a numeric value or iterable

Returns:
    The hyperbolic arc-cosine value of `x`
""",
)

asinh = _register_trig_hb_func(
    "asinh",
    "arcsinh",
    doc="""Hyperbolic arc-sine

Args:
    x: a numeric value or iterable

Returns:
    The hyperbolic arc-sine value of `x`
""",
)

atanh = _register_trig_hb_func(
    "atanh",
    "arctanh",
    doc="""Hyperbolic arc-tangent

Args:
    x: a numeric value or iterable

Returns:
    The hyperbolic arc-tangent value of `x`
""",
)


@register_func(None, context=Context.EVAL)
def atan2(y: FloatOrIter, x: FloatOrIter) -> FloatOrIter:
    """Calculates the angle between the x-axis and the vector (0,0) -> (x,y)

    Args:
        y: and
        x: The end coordinates of the vector

    Returns:
        The angle between x-axis and vector (0,0) -> (x,y)
    """
    return numpy.arctan2(y, x)
