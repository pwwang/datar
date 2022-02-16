"""Core utilities"""
import sys
import time
import logging
from functools import singledispatch
from typing import TYPE_CHECKING, Any

from pandas.api.types import is_scalar
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from pandas.core.groupby import SeriesGroupBy, DataFrameGroupBy

if TYPE_CHECKING:
    from .tibble import Tibble

# logger
logger = logging.getLogger("datar")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s][%(name)s][%(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(stream_handler)


@singledispatch
def name_of(value) -> str:
    return str(value)


@name_of.register
def _(value: Series) -> str:
    return value.name


@name_of.register
def _(value: SeriesGroupBy) -> str:
    return value.obj.name


@name_of.register
def _(value: DataFrame) -> str:
    return None


def prefice_nest_df(df: DataFrame, prefix: str) -> DataFrame:
    if not prefix:
        return df

    df = df.copy()
    df.columns = [f"{prefix}${col}" for col in df.columns]
    return df


def add_to_tibble(
    tbl: "Tibble",
    name: str,
    value: Any,
    allow_dup_names: bool = False,
) -> "Tibble":
    """Add data to tibble"""
    if value is None:
        return tbl

    from .tibble import Tibble, TibbleGroupby
    from ..tibble import as_tibble

    if tbl is None:
        if isinstance(value, (DataFrameGroupBy, DataFrame)):
            out = as_tibble(value)
            return prefice_nest_df(out, name)

        if isinstance(value, SeriesGroupBy):
            out = TibbleGroupby.from_grouped(value)
            return prefice_nest_df(out, name)

        if isinstance(value, Series):
            return Tibble({name: value})

        if is_scalar(value):
            return Tibble({name: [value]})

        return Tibble({name: value})

    if not allow_dup_names or name not in tbl:
        tbl[name] = value
    else:
        # better way to add a column with duplicated name?
        columns = tbl.columns.values.copy()
        dupcol_idx = tbl.columns.get_indexer_for([name])
        columns[dupcol_idx] = f"{name}_{int(time.time())}"
        tbl.columns = columns
        tbl[name] = value
        columns[dupcol_idx] = name
        tbl.columns = columns.tolist() + [name]

    return tbl
