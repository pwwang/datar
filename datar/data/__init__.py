"""Collects datasets from R-datasets, dplyr and tidyr packages"""
import functools
from typing import Any, List

from ..core.load_plugins import plugin
from .metadata import Metadata, metadata


# Should never do `from datar.data import *`
__all__ = []  # type: List[str]


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
def load_dataset(name: str, __backend: str = None) -> Any:
    """Load the specific dataset"""
    loaded = plugin.hooks.load_dataset(name, metadata, __plugin=__backend)
    if loaded is None:
        from ..core.utils import NotImplementedByCurrentBackendError
        raise NotImplementedByCurrentBackendError(f"loading dataset '{name}'")

    return loaded


def __getattr__(name: str):
    # mkapi accesses quite a lot of attributes starting with _
    if not name.isidentifier() or name.startswith("__"):  # pragma: no cover
        raise AttributeError(name)

    return load_dataset(name.lower())
