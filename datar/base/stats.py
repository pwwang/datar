# r-stats
from typing import Mapping
from functools import singledispatch

import numpy as np
from pandas import DataFrame, Series
from pandas.api.types import is_scalar

from datar.core.tibble import TibbleGrouped, TibbleRowwise

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


@_rnorm.register(TibbleGrouped)
def _rnorm_grouped(args_frame, n, mean, sd, raw_n):
    return args_frame._datar["grouped"].apply(
        lambda subdf: np.random.normal(
            subdf["mean"], subdf["sd"], subdf.shape[0]
        )
    )


@_rnorm.register(TibbleRowwise)
def _rnorm_rowwise(args_frame, n, mean, sd, raw_n):
    return args_frame.agg(
        lambda row: np.random.normal(row["mean"], row["sd"], row["n"]), axis=1
    )


@func_factory(None, {"n", "mean", "sd"})
def rnorm(
    n: Series,
    mean: Series = 0.0,
    sd: Series = 1.0,
    __args_raw: Mapping = None,
    __args_frame: DataFrame = None,
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
    return _rnorm(__args_frame, n, mean, sd, __args_raw["n"])


@singledispatch
def _runif(args_frame, n, min, max, raw_n):
    if is_scalar(raw_n):
        scalar_n = raw_n
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


@_runif.register(TibbleGrouped)
def _runif_grouped(args_frame, n, min, max, raw_n):
    return args_frame._datar["grouped"].apply(
        lambda subdf: np.random.uniform(
            low=subdf["min"],
            high=subdf["max"],
            size=subdf.shape[0],
        )
    )


@_runif.register(TibbleRowwise)
def _runif_rowwise(
    args_frame,
    n,
    min,
    max,
    raw_n,
):  # pragma: no cover.  pytest-cov can't hit this, but it's tested
    return args_frame.agg(
        lambda row: np.random.uniform(
            low=row["min"], high=row["max"], size=row["n"]
        ),
        axis=1,
    )


@func_factory(None, {"n", "min", "max"})
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
    return _runif(__args_frame, n, min, max, __args_raw["n"])


@singledispatch
def _rpois(args_frame, n, lambda_, raw_n):
    if is_scalar(raw_n):
        scalar_n = raw_n
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


@_rpois.register(TibbleGrouped)
def _rpois_grouped(args_frame, n, lambda_, raw_n):
    return args_frame._datar["grouped"].apply(
        lambda subdf: np.random.poisson(
            lam=subdf["lambda_"],
            size=subdf.shape[0],
        )
    )


@_rpois.register(TibbleRowwise)
def _rpois_rowwise(args_frame, n, lambda_, raw_n):
    return args_frame.agg(
        lambda row: np.random.poisson(lam=row["lambda_"], size=row["n"]),
        axis=1,
    )


@func_factory(None, {"n", "lambda_"})
def rpois(
    n: Series,
    lambda_: Series = 0.0,
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
    return _rpois(__args_frame, n, lambda_, __args_raw["n"])
