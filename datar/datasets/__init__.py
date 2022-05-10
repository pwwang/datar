"""Collects datasets from R-datasets, dplyr and tidyr packages"""
import functools
from pathlib import Path
from typing import List

from ..core.backends import pandas as pd
import rtoml

HERE = Path(__file__).parent


@functools.lru_cache()
def list_datasets():
    """Get the information of all datasets"""
    with HERE.joinpath("metadata.toml").open() as fmd:
        return rtoml.load(fmd)


@functools.lru_cache()
def load_data(name: str) -> pd.DataFrame:
    """Load the specific dataset"""
    datasets = list_datasets()
    try:
        metadata = datasets[name]
    except KeyError:
        raise ImportError(
            f"No such dataset: {name}, "
            f"available: {list(datasets)}"
        ) from None

    data = pd.read_csv(
        HERE / metadata["source"],
        index_col=0 if metadata["index"] else False,
    )
    return data


__all__ = []  # type: List[str]


def __getattr__(name):
    # mkapi accesses quite a lot of attributes starting with _
    try:
        return load_data(name.lower())
    except ImportError as imerr:
        raise AttributeError from imerr
