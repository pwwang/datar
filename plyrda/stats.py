"""Some functions ported from R-stats"""

from pipda import Context
from plyrda.utils import register_grouped
from typing import Any, Iterable, List, Union

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

@register_grouped(context=Context.EVAL)
def quantile(
        series: Iterable[Any],
        probs: Union[float, Iterable[float]] = (0.0, 0.25, 0.5, 0.75, 1.0),
        na_rm: bool = False
):
    return (
        numpy.nanquantile(series, probs) if na_rm
        else numpy.quantile(series, probs)
    )
