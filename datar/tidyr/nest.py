"""Nest and unnest

https://github.com/tidyverse/tidyr/blob/master/R/nest.R
"""
from typing import Callable, Mapping, Union, Iterable, List
import re

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar
from ..core.backends.pandas.core.generic import NDFrame
from pipda import register_verb

from ..core.utils import vars_select
from ..core.broadcast import broadcast_to, init_tibble_from
from ..core.tibble import (
    Tibble,
    TibbleGrouped,
    TibbleRowwise,
    reconstruct_tibble,
)
from ..core.contexts import Context

from ..base import setdiff, NA
from ..dplyr import (
    distinct,
    bind_cols,
    group_vars,
    group_data,
    arrange,
    ungroup,
)

from .pack import unpack
from .chop import unchop


@register_verb(DataFrame, context=Context.SELECT)
def nest(
    _data: DataFrame,
    _names_sep: str = None,
    **cols: Union[str, int],
) -> DataFrame:
    """Nesting creates a list-column of data frames

    Args:
        _data: A data frame
        **cols: Columns to nest
        _names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `_names_sep`.

    Returns:
        Nested data frame.
    """
    if not cols:
        raise ValueError("`**cols` must not be empty.")

    all_columns = _data.columns
    colgroups = {}
    usedcols = set()
    for group, columns in cols.items():
        old_cols = all_columns[vars_select(all_columns, columns)]
        usedcols = usedcols.union(old_cols)
        newcols = (
            old_cols
            if _names_sep is None
            else _strip_names(old_cols, group, _names_sep)
        )
        colgroups[group] = dict(zip(newcols, old_cols))

    asis = setdiff(_data.columns, list(usedcols), __ast_fallback="normal")
    keys = _data[asis]
    u_keys = distinct(keys, __ast_fallback="normal").reset_index(drop=True)
    nested = []
    for group, columns in colgroups.items():
        if _names_sep is None:  # names as is
            # out <- map(cols, ~ vec_split(.data[.x], keys)$val)
            val = _vec_split(_data[list(columns)], keys).val
        else:
            # out <- map(
            #   cols,
            #   ~ vec_split(set_names(.data[.x], names(.x)), keys)$val
            # )
            to_split = _data[list(columns.values())]
            to_split.columns = list(columns)
            val = _vec_split(to_split, keys).val
        nested.append(val.reset_index(drop=True))

    out = pd.concat(nested, ignore_index=True, axis=1)
    out.columns = list(colgroups)
    if u_keys.shape[1] == 0:
        return out if isinstance(out, DataFrame) else out.to_frame()

    return bind_cols(
        u_keys,
        broadcast_to(out, u_keys.index),
        __ast_fallback="normal",
    )


@nest.register(TibbleGrouped, context=Context.SELECT)
def _(
    _data: TibbleGrouped,
    _names_sep: str = None,
    **cols: Mapping[str, Union[str, int]],
) -> TibbleGrouped:
    """Nesting grouped dataframe"""
    if not cols:
        cols = {
            "data": setdiff(
                _data.columns,
                group_vars(_data, __ast_fallback="normal"),
                __ast_fallback="normal",
            )
        }
    out = nest.dispatch(DataFrame)(
        ungroup(_data, __ast_fallback="normal"),
        **cols,
        _names_sep=_names_sep,
    )
    return reconstruct_tibble(_data, out)


@register_verb(DataFrame, context=Context.SELECT)
def unnest(
    data: DataFrame,
    *cols: Union[str, int],
    keep_empty: bool = False,
    dtypes=None,
    names_sep: str = None,
    names_repair: Union[str, Callable] = "check_unique",
) -> DataFrame:
    """Flattens list-column of data frames back out into regular columns.

    Args:
        data: A data frame to flatten.
        *cols: Columns to unnest.
        keep_empty: By default, you get one row of output for each element
            of the list your unchopping/unnesting.
            This means that if there's a size-0 element
            (like NULL or an empty data frame), that entire row will be
            dropped from the output.
            If you want to preserve all rows, use `keep_empty` = `True` to
            replace size-0 elements with a single row of missing values.
        dtypes: Providing the dtypes for the output columns.
            Could be a single dtype, which will be applied to all columns, or
            a dictionary of dtypes with keys for the columns and values the
            dtypes.
        names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `names_sep`.
        names_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        Data frame with selected columns unnested.
    """
    if not cols:
        raise ValueError("`*cols` is required when using unnest().")

    all_columns = data.columns
    cols = vars_select(all_columns, cols)
    cols = all_columns[cols]

    out = ungroup(data, __ast_fallback="normal")

    for col in cols:
        out[col] = _as_df(data[col])

    out = unchop(
        out,
        cols,
        keep_empty=keep_empty,
        dtypes=dtypes,
        __ast_fallback="normal",
    )
    out = unpack(
        out,
        cols,
        names_sep=names_sep,
        names_repair=names_repair,
        __ast_fallback="normal",
    )
    return reconstruct_tibble(data, out)


@unnest.register(TibbleRowwise, context=Context.SELECT)
def _(
    data: TibbleRowwise,
    *cols: Union[str, int],
    keep_empty: bool = False,
    dtypes=None,
    names_sep: str = None,
    names_repair: Union[str, Callable] = "check_unique",
) -> TibbleGrouped:
    """Unnest rowwise dataframe"""
    out = unnest.dispatch(DataFrame)(
        ungroup(data, __ast_fallback="normal"),
        *cols,
        keep_empty=keep_empty,
        dtypes=dtypes,
        names_sep=names_sep,
        names_repair=names_repair,
    )
    if not data.group_vars:
        return out

    grouped = data._datar["grouped"]
    return TibbleGrouped.from_groupby(
        out.groupby(
            data.group_vars,
            observed=grouped.observed,
            sort=grouped.sort,
            dropna=grouped.dropna,
        )
    )


def _strip_names(names: Iterable[str], base: str, sep: str) -> List[str]:
    """Strip the base names with sep"""
    out = []
    for name in names:
        if not sep:
            out.append(name[len(base) :] if name.startswith(base) else name)
        else:
            parts = re.split(re.escape(sep), name, maxsplit=1)
            out.append(parts[1] if parts[0] == base else name)
    return out


def _as_df(series: Series) -> List[DataFrame]:
    """Convert series to dataframe"""
    out = []
    for val in series:
        if isinstance(val, DataFrame):
            if val.shape[1] == 0:  # no columns
                out.append(NA)
            elif val.shape[0] == 0:
                out.append(
                    DataFrame([[NA] * val.shape[1]], columns=val.columns)
                )
            else:
                out.append(val)
        elif is_scalar(val) and pd.isnull(val):
            out.append(val)
        else:
            out.append(
                init_tibble_from(val, name=getattr(series, "obj", series).name)
            )
    return out


def _vec_split(x: NDFrame, by: NDFrame) -> DataFrame:
    """Split a vector into groups

    Returns a data frame with columns `key` and `val`. `key` is a stacked column
    with data from by.
    """
    if isinstance(x, Series):  # pragma: no cover, always a data frame?
        x = x.to_frame()
    if isinstance(by, Series):  # pragma: no cover, always a data frame?
        by = by.to_frame()

    df = bind_cols(x, by, __ast_fallback="normal")
    if df.shape[0] == 0:
        return Tibble(columns=["key", "val"])
    if by.shape[1] > 0:
        if not isinstance(df, Tibble):  # pragma: no cover
            df = Tibble(df, copy=False)
        df = df.group_by(by.columns.tolist(), drop=True)

    gdata = group_data(df, __ast_fallback="normal")
    gdata = arrange(gdata, gdata._rows, __ast_fallback="normal")
    out = Tibble(index=gdata.index)
    out["key"] = gdata[by.columns]
    out["val"] = [
        x.iloc[rows, :].reset_index(drop=True) for rows in gdata._rows
    ]
    return out
