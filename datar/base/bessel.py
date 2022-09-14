"""Bessel function family"""
from ..core.factory import func_factory


def _get_special_func_from_scipy(name):
    """Import bessel functions from scipy on the fly

    In order to make scipy dependency optional
    """
    try:
        from scipy import special
    except ImportError as imperr:  # pragma: no cover
        raise ValueError(
            "`bessel` family requires `scipy` package.\n"
            "Try: pip install -U datar[scipy]"
        ) from imperr

    return getattr(special, name)


def _register_bessel_function(
    name,
    common_fun,
    faster_fun,
    exp_fun=None,
    doc="",
):
    """Register bessel function"""

    if exp_fun is None:

        @func_factory(
            kind="transform",
            name=name,
            qualname=f"datar.base.{name}",
            doc=doc,
        )
        def bessel_fun(x, nu):
            """Bessel function"""
            # use faster version for order 0 and 1
            if nu in faster_fun:
                fun = _get_special_func_from_scipy(faster_fun[nu])
                return fun(x)

            fun = _get_special_func_from_scipy(common_fun)
            return fun(nu, x)

    else:

        @func_factory(
            kind="transform",
            name=name,
            qualname=f"datar.base.{name}",
            doc=doc,
        )
        def bessel_fun(x, nu, expon_scaled=False):
            """Modified bessel function"""
            # use faster version for order 0 and 1
            if (nu, expon_scaled) in faster_fun:
                fun = _get_special_func_from_scipy(
                    faster_fun[(nu, expon_scaled)]
                )
                return fun(x)

            if expon_scaled:
                fun = _get_special_func_from_scipy(exp_fun)
                return fun(nu, x)

            fun = _get_special_func_from_scipy(common_fun)
            return fun(nu, x)

    return bessel_fun


bessel_j = _register_bessel_function(
    "bessel_j",
    common_fun="jv",
    faster_fun={0.0: "j0", 1.0: "j1"},
    doc="""Bessel function of first kind

    Args:
        x: An iterable with numeric >= 0
        nu: The order of the bessel function

    Returns:
        Numeric iterable with the values of the corresponding
        Bessel function.
    """,
)

bessel_y = _register_bessel_function(
    "bessel_y",
    common_fun="yv",
    faster_fun={0.0: "y0", 1.0: "y1"},
    doc="""Bessel function of second kind

    Args:
        x: An iterable with numeric >= 0
        nu: The order of the bessel function

    Returns:
        Numeric iterable with the values of the corresponding
        Bessel function.
    """,
)

bessel_i = _register_bessel_function(
    "bessel_i",
    common_fun="iv",
    exp_fun="ive",
    faster_fun={
        (0.0, False): "i0",
        (1.0, False): "i1",
        (0.0, True): "i0e",
        (1.0, True): "i1e",
    },
    doc="""Modified bessel function of first kind

    Args:
        x: An iterable with numeric >= 0
        nu: The order of the bessel function
        expon_scaled: if TRUE, the results are exponentially scaled
            in order to avoid overflow

    Returns:
        Numeric iterable with scaled values of the corresponding
        Bessel function.
    """,
)

bessel_k = _register_bessel_function(
    "bessel_k",
    common_fun="kv",
    exp_fun="kve",
    faster_fun={
        (0.0, False): "k0",
        (1.0, False): "k1",
        (0.0, True): "k0e",
        (1.0, True): "k1e",
    },
    doc="""Modified bessel function of first kind

    Args:
        x: An iterable with numeric >= 0
        nu: The order of the bessel function
        expon_scaled: if TRUE, the results are exponentially scaled
            in order to avoid underflow

    Returns:
        Numeric iterable with scaled values of the corresponding
        Bessel function.
    """,
)
