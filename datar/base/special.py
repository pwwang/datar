"""Special mathematical functions related to the beta and gamma functions."""

from pipda import register_func

import numpy as np

from ..core.backends.pandas.api.types import is_scalar
from ..core.contexts import Context
from .bessel import _get_special_func_from_scipy

# beta(a, b)              => scipy.special.beta(a,b)
# lbeta(a, b)             => scipy.special.betaln(a,b)

# gamma(x)                => scipy.special.gamma(x)
# lgamma(x)               => scipy.special.gammaln(x)
# psigamma(x, deriv = 0)  => scipy.special.polygamma(deriv, x)
# digamma(x)              => scipy.special.digamma(x)
# trigamma(x)             => scipy.special.polygamma(1, x)

# choose(n, k)            => scipy.special.comb(n, k)
# lchoose(n, k)           => np.log(scipy.special.comb(n,k))
# factorial(x)            => scipy.special.factoral(x)
# lfactorial(x)           => np.log(scipy.special.factoral(x))


@register_func(context=Context.EVAL)
def beta(a, b):
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


@register_func(context=Context.EVAL)
def lbeta(a, b):
    """The natural logarithm of `beta()`

    Args:
        a: and
        b: Real-valued arguments

    Returns:
        The natural logarithm of value of the beta function
    """
    return np.log(beta(a, b))


@register_func(context=Context.EVAL)
def gamma(x):
    """The gamma function

    Args:
        x: Real or complex valued argument

    Returns:
        Values of the gamma function
    """
    fun = _get_special_func_from_scipy("gamma")
    if is_scalar(x) and x <= 0:
        return np.nan
    if not is_scalar(x):
        x = np.array(x, dtype=float)
        x[x <= 0.0] = np.nan
    return fun(x)


@register_func(context=Context.EVAL)
def lgamma(x):
    """The nature logarithm of `gamma()`

    Args:
        x: Real or complex valued argument

    Returns:
        The natural logarithm of values of the gamma function
    """
    return np.log(gamma(x))


@register_func(context=Context.EVAL)
def digamma(x):
    """The digamma function.

    Args:
        x: Real or complex valued argument

    Returns:
        Computed values of psi.
    """
    fun = _get_special_func_from_scipy("digamma")
    if x == 0.0:
        return np.nan
    return fun(x)


@register_func(context=Context.EVAL)
def choose(n, k):
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


@register_func(context=Context.EVAL)
def lchoose(n, k):
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
    return np.log(choose(n, k))


@register_func(context=Context.EVAL)
def factorial(x):
    """The factorial of a number or array of numbers.

    Args:
        Input values. If x < 0, the return value is np.nan.

    Returns:
        Factorial of x
    """
    fun = _get_special_func_from_scipy("factorial")
    out = fun(x)

    if is_scalar(x):
        out = np.nan if x < 0 else out
        return out

    out[np.array(x) < 0] = np.nan
    return out


@register_func(context=Context.EVAL)
def lfactorial(x):
    """The natural logarithm of `factorial()`

    Args:
        Input values. If x < 0, the return value is np.nan.

    Returns:
        The natural logarithm of factorial of x
    """
    return np.log(factorial(x))


@register_func(context=Context.EVAL)
def trigamma(x):
    """The second derivatives of the logarithm of the gamma function

    Args:
        x: A numeric value or iterable

    Returns:
        The value of the 2nd derivatives of the logarithm of the gamma function
    """
    fun = _get_special_func_from_scipy("polygamma")
    return fun(1, x)


@register_func(context=Context.EVAL)
def psigamma(x, deriv):
    """The deriv-th derivatives of the logarithm of the gamma function

    Args:
        x: A numeric value or iterable

    Returns:
        The value of the 2nd derivatives of the logarithm of the gamma function
    """
    fun = _get_special_func_from_scipy("polygamma")
    deriv = np.round(deriv)
    return fun(deriv, x)
