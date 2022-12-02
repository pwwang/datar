"""Collects datasets from R-datasets, dplyr and tidyr packages"""
import functools
from typing import List

from ..apis.data import load_dataset as _load_dataset
from ..core.plugin import plugin
from .metadata import Metadata, metadata


# Should never do `from datar.data import *`
__all__ = []  # type: List[str]

# Load implementations for _load_dataset
plugin.hooks.data_api()


def descr_datasets(*names: str):
    """Get the information of the given datasets

    Args:
        *names: Names of the datasets to get the information of.
    """
    return {
        key: val
        for key, val in metadata.items()
        if key in names or not names
    }


def add_dataset(name: str, meta: Metadata):
    """Add a dataset to the registry

    Args:
        name: The name of the dataset
        metadata: The metadata of the dataset
    """
    metadata[name] = meta


@functools.lru_cache()
def load_dataset(name: str, __backend: str = None):
    """Load the specific dataset"""
    if name not in metadata:
        raise ImportError(
            f"No such dataset: {name}, "
            f"available: {list(metadata)}"
        ) from None

    loaded = _load_dataset(name, metadata[name], __backend=__backend)
    if loaded is None:
        from ..core.utils import NotImplementedByCurrentBackendError
        raise NotImplementedByCurrentBackendError(f"loading dataset '{name}'")

    return loaded


def __getattr__(name):
    # mkapi accesses quite a lot of attributes starting with _
    try:
        return load_dataset(name.lower())
    except ImportError as imerr:
        raise AttributeError from imerr
