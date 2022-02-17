"""Core utilities"""
import sys
import time
import logging
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Callable, Iterable, Sequence, Tuple

import numpy as np
from pipda.utils import CallingEnvs
from pandas.api.types import is_scalar
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from pandas.core.groupby import SeriesGroupBy, DataFrameGroupBy

if TYPE_CHECKING:
    from pandas.core.groupby.ops import BaseGrouper
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

    if name is None and isinstance(value, DataFrame):
        for col in value.columns:
            tbl = add_to_tibble(tbl, col, value[col], allow_dup_names)
        return tbl

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


def regcall(func: Callable, *args: Any, **kwargs: Any) -> Any:
    """Call function with regular calling env"""
    return func(*args, **kwargs, __calling_env=CallingEnvs.REGULAR)


def ensure_nparray(x: Any) -> np.ndarray:
    if is_scalar(x):
        return np.array([x])

    if not isinstance(x, np.ndarray):
        return np.array(x)

    return x


def arg_match(
    arg: Any, argname: str, values: Iterable, errmsg: str = None,
) -> Any:
    """Make sure arg is in one of the values.

    Mimics `rlang::arg_match`.
    """
    if not errmsg:
        values = list(values)
        errmsg = f"`{argname}` must be one of {values}."
    if arg not in values:
        raise ValueError(errmsg)
    return arg


def vars_select(
    all_columns: Iterable[str],
    *columns: Any,
    raise_nonexists: bool = True,
) -> Sequence[int]:
    # TODO: support selecting data-frame columns
    """Select columns

    Args:
        all_columns: The column pool to select
        *columns: arguments to select from the pool
        raise_nonexist: Whether raise exception when column not exists
            in the pool

    Returns:
        The selected indexes for columns

    Raises:
        KeyError: When the column does not exist in the pool
            and raise_nonexists is True.
    """
    from .collections import Collection
    from ..base import unique

    columns = [
        column.name
        if isinstance(column, Series)
        else column.obj.name
        if isinstance(column, SeriesGroupBy)
        else column
        for column in columns
    ]
    selected = Collection(*columns, pool=list(all_columns))
    if raise_nonexists and selected.unmatched and selected.unmatched != {None}:
        raise KeyError(
            f"Columns `{selected.unmatched}` do not exist."
        )
    return unique(selected).astype(int)


def _broadcast_to_grouped(value: Any, grouped: SeriesGroupBy) -> Any:
    """Broadcast a value to a grouped series object"""
    if is_scalar(value):
        return value

    if isinstance(value, Series):
        result_index = grouped.grouper.result_index
        index = grouped.obj.index
        # Aggregated results like f.x.mean()
        if value.index.equals(result_index):
            # broadcast values in all groups
            out = Series(
                value,
                index=result_index.repeat(grouped.grouper.size()),
            )
            out.index = index
            return out
        if value.index.equals(index):
            return value

        logger.warning("Incompatible Series, ignoring index.")

    usizes = grouped.grouper.size().unique()
    if len(usizes) > 1:
        for usize in usizes:
            if len(value) != usize:
                raise ValueError(
                    f"Length of values ({len(value)}) does not match "
                    f"length of index ({usize})"
                )

    usize = usizes[0]
    if len(value) != usize:
        raise ValueError(
            f"Length of values ({len(value)}) does not match "
            f"length of index ({usize})"
        )

    return Series(np.tile(value, grouped.ngroups))


# multiple dispatch?
def broadcast(value1: Any, value2: Any) -> Tuple[Any, Any, "BaseGrouper"]:
    """Broadcast value1 to the dimension of value2 or vice versa


    """
    if isinstance(value1, SeriesGroupBy) and isinstance(value2, SeriesGroupBy):
        if value1.grouper is not value2.grouper:
            raise ValueError(
                "Cannot broadcast a SeriesGroupBy object to another "
                "with a different grouper."
            )
        return value1.obj, value2.obj, value1.grouper

    if isinstance(value1, SeriesGroupBy):
        value2 = _broadcast_to_grouped(value2, value1)
        return value1.obj, value2, value1.grouper

    if isinstance(value2, SeriesGroupBy):
        value1 = _broadcast_to_grouped(value1, value2)
        return value1, value2.obj, value2.grouper

    if is_scalar(value1) or is_scalar(value2):
        return value1, value2, None

    if len(value1) == 1:
        value1 = [value1[0]] * len(value2)
        return value1, value2, None

    if len(value2) == 1:
        value2 = [value2[0]] * len(value1)
        return value1, value2, None

    return value1, value2, None
