"""Some functions ported from R-stats"""
from typing import Any, Iterable, List, Optional

import numpy
from pipda import register_func

from ..core.types import (
    FloatOrIter, NumericOrIter, NumericType, SeriesLikeType, is_scalar
)
from ..core.contexts import Context

# pylint: disable=redefined-builtin, redefined-outer-name

@register_func(None, context=Context.EVAL)
def rnorm(n: int, mean: float = 0.0, sd: float = 1.0) -> List[float]:
    """random generation for the normal distribution with mean equal to mean
    and standard deviation equal to sd.

    Args:
        n: number of observations. No iterables allowed
        mean: means. No iterables allowed
        sd: standard deviations. No iterables allowed


    Returns:
        Randomly generated deviates.
    """
    return numpy.random.normal(loc=mean, scale=sd, size=n)

def runif(n: int, min: float = 0.0, max: float = 1.0) -> List[float]:
    """random generation for the uniform distribution

    Args:
        n: number of observations. No iterables allowed
        min: the minima. No iterables allowed
        max: the maxima. No iterables allowed


    Returns:
        Randomly generated deviates.
    """
    return numpy.random.uniform(low=min, high=max, size=n)

def rpois(n: int, lambda_: float) -> List[float]:
    """random generation for the Poisson distribution with parameter lambda_.

    Args:
        n: number of random values to return.
        mean: non-negative means.


    Returns:
        Randomly generated deviates.
    """
    return numpy.random.poisson(lam=lambda_, size=n)

@register_func(None, context=Context.EVAL)
def quantile(
        series: Iterable[Any],
        probs: FloatOrIter = (0.0, 0.25, 0.5, 0.75, 1.0),
        na_rm: bool = False
) -> SeriesLikeType:
    """produces sample quantiles corresponding to the given probabilities.

    Args:
        series: The data to sample
        probs: numeric vector of probabilities with values in [0,1]
        na_rm: if true, any ‘NA’ and ‘NaN’'s are removed from ‘x’
            before the quantiles are computed.

    Returns:
        An array of quantile values
    """
    return (
        numpy.nanquantile(series, probs) if na_rm
        else numpy.quantile(series, probs)
    )

@register_func(None, context=Context.EVAL)
def sd(
        series: Iterable[Any],
        na_rm: bool = False,
        # numpy default is 0. Make it 1 to be consistent with R
        ddof: int = 1
) -> float:
    """Get standard deviation of the input"""
    return (
        numpy.nanstd(series, ddof=ddof) if na_rm
        else numpy.std(series, ddof=ddof)
    )

@register_func(None, context=Context.EVAL)
def weighted_mean(
        # pylint: disable=invalid-name
        x: NumericOrIter,
        w: Optional[NumericOrIter] = None,
        na_rm: bool = False
) -> NumericType:
    """Calculate weighted mean"""
    if is_scalar(x):
        x = [x]
    if w is not None and is_scalar(w):
        w = [w]
    x = numpy.array(x)
    if w is not None:
        w = numpy.array(w)
        if len(x) != len(w):
            raise ValueError("'x' and 'w' must have the same length")

    if na_rm:
        notna = ~numpy.isnan(x)
        x = x[notna]
        if w is not None:
            w = w[notna]

    if w is not None and sum(w) == 0:
        return numpy.nan
    return numpy.average(x, weights=w)
