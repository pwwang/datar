"""Some functions ported from R-stats"""

from typing import List

import numpy

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

def rpois(n: int, lambda_: float) -> List[float]:
    """random generation for the Poisson distribution with parameter lambda_.

    Args:
        n: number of random values to return.
        mean: non-negative means.


    Returns:
        Randomly generated deviates.
    """
    return numpy.random.poisson(lam=lambda_, size=n)
