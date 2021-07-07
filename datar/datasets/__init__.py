"""Collects datasets from R-datasets, dplyr and tidyr packages"""
import functools
from pathlib import Path
from typing import List

import pandas

HERE = Path(__file__).parent


@functools.lru_cache()
def all_datasets():
    """Get the information of all datasets"""
    datasets = {}
    for datafile in HERE.glob("*.csv.gz"):
        index = False
        name = datafile.name[:-7]
        if ".indexed" in name:
            name = name.replace(".indexed", "")
            index = True
        datasets[name] = {"index": index, "file": datafile}
    return datasets


@functools.lru_cache()
def load_data(name: str) -> pandas.DataFrame:
    """Load the specific dataset"""
    datasets = all_datasets()
    if name not in datasets:
        raise ImportError(
            f"No such dataset: {name}, "
            f"available: {list(all_datasets().keys())}"
        )

    dataset = datasets[name]
    data = pandas.read_csv(
        dataset["file"], index_col=0 if dataset["index"] else False
    )
    data.__dfname__ = name
    return data


__all__ = [] # type: List[str]


def __getattr__(name):
    # mkapi accesses quite a lot of attributes starting with _
    if name.startswith("_"):
        raise AttributeError
    return load_data(name)
