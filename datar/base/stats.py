# r-stats
from typing import Mapping
from functools import singledispatch

import numpy as np

from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar

from ..core.tibble import TibbleGrouped, TibbleRowwise
from ..core.factory import func_factory


@singledispatch
def _rnorm(args_frame, n, mean, sd, raw_n):
    if is_scalar(raw_n):
        scalar_n = raw_n
        if scalar_n > n.size:
            ntiles = scalar_n // n.size + 1
            return np.random.normal(
                np.tile(mean, ntiles)[:scalar_n],
                np.tile(sd, ntiles)[:scalar_n],
                scalar_n,
            )

        return np.random.normal(
            mean.values[:scalar_n],
            sd.values[:scalar_n],
            scalar_n,
        )

    return Series(np.random.normal(mean, sd, n.size), index=n.index)


@func_factory({"n", "mean", "sd"})
def rnorm(
    n: Series,
    mean: Series = 0.0,
    sd: Series = 1.0,
    __args_raw: Mapping = None,
) -> Series:
    """random generation for the normal distribution with mean equal to mean
    and standard deviation equal to sd.

    Args:
        n: number of observations.
        mean: means.
        sd: standard deviations.


    Returns:
        Randomly generated deviates.
    """
    if is_scalar(__args_raw["n"]):
        scalar_n = __args_raw["n"]
        if scalar_n > n.size:
            ntiles = scalar_n // n.size + 1
            return np.random.normal(
                np.tile(mean, ntiles)[:scalar_n],
                np.tile(sd, ntiles)[:scalar_n],
                scalar_n,
            )

        return np.random.normal(
            mean.values[:scalar_n],
            sd.values[:scalar_n],
            scalar_n,
        )

    return Series(np.random.normal(mean, sd, n.size), index=n.index)


@func_factory({"n", "min", "max"})
def runif(
    n: Series,
    min: Series = 0.0,
    max: Series = 1.0,
    __args_raw: Mapping = None,
    __args_frame: DataFrame = None,
) -> Series:
    """random generation for the uniform distribution

    Args:
        n: number of observations.
        min: the minima.
        max: the maxima.


    Returns:
        Randomly generated deviates.
    """
    if is_scalar(__args_raw["n"]):
        scalar_n = __args_raw["n"]
        if scalar_n > n.size:
            ntiles = scalar_n // n.size + 1
            return np.random.uniform(
                low=np.tile(min, ntiles)[:scalar_n],
                high=np.tile(max, ntiles)[:scalar_n],
                size=scalar_n,
            )

        return np.random.uniform(
            low=min.values[:scalar_n],
            high=max.values[:scalar_n],
            size=scalar_n,
        )

    return Series(
        np.random.uniform(low=min, high=max, size=n.size),
        index=n.index,
    )


@func_factory({"n", "lambda_"})
def rpois(
    n: Series,
    lambda_: Series = 0.0,
    __args_raw: Mapping = None,
) -> Series:
    """random generation for the uniform distribution

    Args:
        n: number of observations.
        min: the minima.
        max: the maxima.


    Returns:
        Randomly generated deviates.
    """
    if is_scalar(__args_raw["n"]):
        scalar_n = __args_raw["n"]
        if scalar_n > n.size:
            ntiles = scalar_n // n.size + 1
            return np.random.poisson(
                lam=np.tile(lambda_, ntiles)[:scalar_n],
                size=scalar_n,
            )

        return np.random.poisson(
            lam=lambda_.values[:scalar_n],
            size=scalar_n,
        )

    return Series(
        np.random.poisson(lam=lambda_, size=n.size),
        index=n.index,
    )
