"""Some functions ported from R-stats"""
from typing import Sequence

import numpy as np
from pandas._typing import AnyArrayLike
from pipda import register_func

from ..core.contexts import Context

from ._funs import _quantile, _sd, _weighted_mean


@register_func(None, context=Context.EVAL)
def rnorm(n: int, mean: float = 0.0, sd: float = 1.0) -> np.ndarray:
    """random generation for the normal distribution with mean equal to mean
    and standard deviation equal to sd.

    Args:
        n: number of observations. No iterables allowed
        mean: means. No iterables allowed
        sd: standard deviations. No iterables allowed


    Returns:
        Randomly generated deviates.
    """
    return np.random.normal(loc=mean, scale=sd, size=n)


def runif(n: int, min: float = 0.0, max: float = 1.0) -> np.ndarray:
    """random generation for the uniform distribution

    Args:
        n: number of observations. No iterables allowed
        min: the minima. No iterables allowed
        max: the maxima. No iterables allowed


    Returns:
        Randomly generated deviates.
    """
    return np.random.uniform(low=min, high=max, size=n)


def rpois(n: int, lambda_: float) -> np.ndarray:
    """random generation for the Poisson distribution with parameter lambda_.

    Args:
        n: number of random values to return.
        mean: non-negative means.


    Returns:
        Randomly generated deviates.
    """
    return np.random.poisson(lam=lambda_, size=n)


@register_func(None, context=Context.EVAL)
def quantile(
    x: Sequence,
    probs: Sequence[float] = (0.0, 0.25, 0.5, 0.75, 1.0),
    na_rm: bool = True,
) -> AnyArrayLike:
    """produces sample quantiles corresponding to the given probabilities.

    Args:
        x: The data to sample
        probs: numeric vector of probabilities with values in [0,1]
        na_rm: if true, any ‘NA’ and ‘NaN’'s are removed from ‘x’
            before the quantiles are computed.

    Returns:
        An array of quantile values
    """
    return _quantile(x, probs, na_rm=na_rm)


@register_func(None, context=Context.EVAL)
def std(
    x: Sequence,
    na_rm: bool = True,
    # numpy default is 0. Make it 1 to be consistent with R
    ddof: int = 1,
) -> float:
    """Get standard deviation of the input"""
    return _sd(x, na_rm=na_rm, ddof=ddof)


sd = std


@register_func(None, context=Context.EVAL)
def weighted_mean(
    x: Sequence,
    w: Sequence = None,
    na_rm: bool = True,
) -> np.ndarray:
    """Calculate weighted mean"""
    return _weighted_mean(x, w, na_rm)
