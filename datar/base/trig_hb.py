"""Trigonometric and Hyperbolic Functions"""
import numpy as np


from ..core.tibble import TibbleGrouped, TibbleRowwise
from ..core.factory import func_factory

from .constants import pi
from .arithmetic import SINGLE_ARG_SIGNATURE


def _register_trig_hb_func(name, doc, np_name=None):
    """Register trigonometric and hyperbolic function"""
    np_name = np_name or name
    np_fun = getattr(np, np_name)
    if name.endswith("pi"):
        func = lambda x: np_fun(x * pi)
    else:
        func = np_fun

    return func_factory(
        kind="transform",
        qualname=name,
        module="datar.base",
        name=name,
        doc=doc,
        func=func,
        signature=SINGLE_ARG_SIGNATURE
    )


sin = _register_trig_hb_func(
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
    doc="""The cosine function

    Args:
        x: a numeric value or iterable

    Returns:
        The cosine value of `x`
    """,
)

tan = _register_trig_hb_func(
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
    np_name="arccos",
    doc="""The arc-cosine function

    Args:
        x: a numeric value or iterable

    Returns:
        The arc-cosine value of `x`
    """,
)

asin = _register_trig_hb_func(
    "acos",
    np_name="arcsin",
    doc="""The arc-sine function

    Args:
        x: a numeric value or iterable

    Returns:
        The arc-sine value of `x`
    """,
)

atan = _register_trig_hb_func(
    "acos",
    np_name="arctan",
    doc="""The arc-sine function

    Args:
        x: a numeric value or iterable

    Returns:
        The arc-sine value of `x`
    """,
)

sinpi = _register_trig_hb_func(
    "sinpi",
    np_name="sin",
    doc="""The sine function

    Args:
        x: a numeric value or iterable, which is the multiple of pi

    Returns:
        The sine value of `x`
    """,
)

cospi = _register_trig_hb_func(
    "cospi",
    np_name="cos",
    doc="""The cosine function

    Args:
        x: a numeric value or iterable, which is the multiple of pi

    Returns:
        The cosine value of `x`
    """,
)

tanpi = _register_trig_hb_func(
    "tanpi",
    np_name="tan",
    doc="""The tangent function

    Args:
        x: a numeric value or iterable, which is the multiple of pi

    Returns:
        The tangent value of `x`
    """,
)

cosh = _register_trig_hb_func(
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
    doc="""Hyperbolic sine

    Args:
        x: a numeric value or iterable

    Returns:
        The hyperbolic sine value of `x`
    """,
)

tanh = _register_trig_hb_func(
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
    np_name="arccosh",
    doc="""Hyperbolic arc-cosine

    Args:
        x: a numeric value or iterable

    Returns:
        The hyperbolic arc-cosine value of `x`
    """,
)

asinh = _register_trig_hb_func(
    "asinh",
    np_name="arcsinh",
    doc="""Hyperbolic arc-sine

    Args:
        x: a numeric value or iterable

    Returns:
        The hyperbolic arc-sine value of `x`
    """,
)

atanh = _register_trig_hb_func(
    "atanh",
    np_name="arctanh",
    doc="""Hyperbolic arc-tangent

    Args:
        x: a numeric value or iterable

    Returns:
        The hyperbolic arc-tangent value of `x`
    """,
)


@func_factory({"y", "x"})
def atan2(y, x, __args_frame=None):
    """Calculates the angle between the x-axis and the vector (0,0) -> (x,y)

    Args:
        y: and
        x: The end coordinates of the vector

    Returns:
        The angle between x-axis and vector (0,0) -> (x,y)
    """
    return np.arctan2(y, x)


@atan2.register(
    (TibbleGrouped, TibbleRowwise),
    func="default",
    post="decor",
)
def _atan2_post(__out, y, x, __args_frame):
    grouped = __args_frame._datar["grouped"]
    out = __out.groupby(
        grouped.grouper,
        sort=grouped.sort,
        dropna=grouped.dropna,
        observed=grouped.observed,
    )
    if isinstance(__args_frame, TibbleRowwise):
        out.is_rowwise = True
    return out
