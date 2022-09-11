"""Core utilities"""
import sys
import logging
import inspect
import textwrap
from functools import singledispatch

import numpy as np

from .backends import pandas as pd
from .backends.pandas import DataFrame, Series
from .backends.pandas.api.types import is_scalar
from .backends.pandas.core.groupby import SeriesGroupBy


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
def name_of(value):
    out = str(value)
    out = textwrap.shorten(out, 16, break_on_hyphens=False, placeholder="...")
    return out


@name_of.register(Series)
def _(value):
    return value.name


@name_of.register(SeriesGroupBy)
def _(value):
    return value.obj.name


@name_of.register(DataFrame)
def _(value):
    return None


def ensure_nparray(x, dtype=None):
    if is_scalar(x):
        return np.array(x, dtype=dtype).ravel()

    if isinstance(x, dict):
        return np.array(list(x), dtype=dtype)

    if not isinstance(x, np.ndarray):
        return np.array(x, dtype=dtype)

    return x


def arg_match(arg, argname, values, errmsg=None):
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
    all_columns,
    *columns,
    raise_nonexists=True,
):
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

    # if not isinstance(all_columns, Index):
    #     all_columns = Index(all_columns, dtype=object)

    columns = [
        column.name
        if isinstance(column, Series)
        else column.obj.name
        if isinstance(column, SeriesGroupBy)
        else column
        for column in columns
    ]
    for col in columns:
        if not isinstance(col, str):
            continue
        colidx = all_columns.get_indexer_for([col])
        if colidx.size > 1:
            raise ValueError(
                "Names must be unique. Name "
                f'"{col}" found at locations {list(colidx)}.'
            )

    selected = Collection(*columns, pool=list(all_columns))
    if raise_nonexists and selected.unmatched and selected.unmatched != {None}:
        raise KeyError(f"Columns `{selected.unmatched}` do not exist.")
    return unique(selected, __ast_fallback="normal").astype(int)


def nargs(fun):
    """Get the number of arguments of a function"""
    return len(inspect.signature(fun).parameters)


def apply_dtypes(df: DataFrame, dtypes) -> None:
    """Apply dtypes to data frame"""
    if dtypes is None or dtypes is False:
        return

    if dtypes is True:
        inferred = df.convert_dtypes()
        for col in df:
            df[col] = inferred[col]
        return

    if not isinstance(dtypes, dict):
        dtypes = dict(zip(df.columns, [dtypes] * df.shape[1]))  # type: ignore

    for column, dtype in dtypes.items():
        if column in df:
            df[column] = df[column].astype(dtype)
        else:
            for col in df:
                if col.startswith(f"{column}$"):
                    df[col] = df[col].astype(dtype)


def dict_get(d, key, default=sys):
    """Get value from dict in case nan is in the key"""
    try:
        return d[key]
    except KeyError:
        if pd.isnull(key):
            for k, v in d.items():
                if pd.isnull(k):
                    return v

        if default is sys:
            raise
        return default
