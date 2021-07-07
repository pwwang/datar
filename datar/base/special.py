"""Special mathematical functions related to the beta and gamma functions."""

import numpy
from pipda import register_func

from ..core.types import FloatOrIter, IntOrIter, NumericOrIter, is_scalar
from ..core.contexts import Context
from ..core.utils import Array
from .bessel import _get_special_func_from_scipy
from .na import NA

# beta(a, b)              => scipy.special.beta(a,b)
# lbeta(a, b)             => scipy.special.betaln(a,b)

# gamma(x)                => scipy.special.gamma(x)
# lgamma(x)               => scipy.special.gammaln(x)
# psigamma(x, deriv = 0)  => scipy.special.polygamma(deriv, x)
# digamma(x)              => scipy.special.digamma(x)
# trigamma(x)             => scipy.special.polygamma(1, x)

# choose(n, k)            => scipy.special.comb(n, k)
# lchoose(n, k)           => numpy.log(scipy.special.comb(n,k))
# factorial(x)            => scipy.special.factoral(x)
# lfactorial(x)           => numpy.log(scipy.special.factoral(x))

# pylint: disable=invalid-name


@register_func(None, context=Context.EVAL)
def beta(a: FloatOrIter, b: FloatOrIter) -> FloatOrIter:
    """The beta function

    Note that when both `a` and `b` are iterables, the broadcast mechanism is
    different from R:
        >>> a = [1,2]; b = [3, 4, 5] will turn into
        >>> [(1, 3), (2, 4), (1, 5)] in R but raises error in python

    Args:
        a: and
        b: Real-valued arguments

    Returns:
        Value of the beta function
    """
    fun = _get_special_func_from_scipy("beta")
    return fun(a, b)


@register_func(None, context=Context.EVAL)
def lbeta(a: FloatOrIter, b: FloatOrIter) -> FloatOrIter:
    """The natural logarithm of `beta()`

    Args:
        a: and
        b: Real-valued arguments

    Returns:
        The natural logarithm of value of the beta function
    """
    return numpy.log(beta(a, b))


@register_func(None, context=Context.EVAL)
def gamma(x: FloatOrIter) -> FloatOrIter:
    """The gamma function

    Args:
        x: Real or complex valued argument

    Returns:
        Values of the gamma function
    """
    fun = _get_special_func_from_scipy("gamma")
    if is_scalar(x) and x <= 0:
        return NA
    if not is_scalar(x):
        x = Array(x, dtype=float)
        x[x <= 0.0] = NA
    return fun(x)


@register_func(None, context=Context.EVAL)
def lgamma(x: FloatOrIter) -> FloatOrIter:
    """The nature logarithm of `gamma()`

    Args:
        x: Real or complex valued argument

    Returns:
        The natural logarithm of values of the gamma function
    """
    return numpy.log(gamma(x))


@register_func(None, context=Context.EVAL)
def digamma(x: FloatOrIter) -> FloatOrIter:
    """The digamma function.

    Args:
        x: Real or complex valued argument

    Returns:
        Computed values of psi.
    """
    fun = _get_special_func_from_scipy("digamma")
    if x == 0.0:
        return NA
    return fun(x)


@register_func(None, context=Context.EVAL)
def choose(n: FloatOrIter, k: IntOrIter) -> FloatOrIter:
    """The number of combinations of N things taken k at a time.

    Note that when both `a` and `b` are iterables, the broadcast mechanism is
    different from R:
        >>> a = [1,2]; b = [3, 4, 5] will turn into
        >>> [(1, 3), (2, 4), (1, 5)] in R but raises error in python

    Args:
        n: Number of things.
        k: Number of elements taken.

    Returns:
        The total number of combinations.
    """
    fun = _get_special_func_from_scipy("comb")
    return fun(n, k)


@register_func(None, context=Context.EVAL)
def lchoose(n: FloatOrIter, k: IntOrIter) -> FloatOrIter:
    """The natural logarithm of `choose()`

    Note that when both `a` and `b` are iterables, the broadcast mechanism is
    different from R:
        >>> a = [1,2]; b = [3, 4, 5] will turn into
        >>> [(1, 3), (2, 4), (1, 5)] in R but raises error in python

    Args:
        n: Number of things.
        k: Number of elements taken.

    Returns:
        The natural logarithm of the total number of combinations.
    """
    return numpy.log(choose(n, k))


@register_func(None, context=Context.EVAL)
def factorial(x: FloatOrIter) -> FloatOrIter:
    """The factorial of a number or array of numbers.

    Args:
        Input values. If x < 0, the return value is NA.

    Returns:
        Factorial of x
    """
    fun = _get_special_func_from_scipy("factorial")
    out = fun(x)

    if is_scalar(x):
        out = NA if x < 0 else out
        return out

    out[Array(x) < 0] = NA
    return out


@register_func(None, context=Context.EVAL)
def lfactorial(x: FloatOrIter) -> FloatOrIter:
    """The natural logarithm of `factorial()`

    Args:
        Input values. If x < 0, the return value is NA.

    Returns:
        The natural logarithm of factorial of x
    """
    return numpy.log(factorial(x))


@register_func(None, context=Context.EVAL)
def trigamma(x: FloatOrIter) -> FloatOrIter:
    """The second derivatives of the logarithm of the gamma function

    Args:
        x: A numeric value or iterable

    Returns:
        The value of the 2nd derivatives of the logarithm of the gamma function
    """
    fun = _get_special_func_from_scipy("polygamma")
    return fun(1, x)


@register_func(None, context=Context.EVAL)
def psigamma(x: FloatOrIter, deriv: NumericOrIter) -> FloatOrIter:
    """The deriv-th derivatives of the logarithm of the gamma function

    Args:
        x: A numeric value or iterable

    Returns:
        The value of the 2nd derivatives of the logarithm of the gamma function
    """
    fun = _get_special_func_from_scipy("polygamma")
    deriv = numpy.round(deriv)
    return fun(deriv, x)
