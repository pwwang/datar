"""Nest and unnest

https://github.com/tidyverse/tidyr/blob/master/R/nest.R
"""
from typing import Callable, Mapping, Union, Iterable, List
import re

import pandas
from pandas import DataFrame, Series
from pipda import register_verb

from ..core.types import Dtype, is_scalar
from ..core.utils import vars_select, recycle_value, to_df, reconstruct_tibble
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.contexts import Context

from ..base import setdiff, NA
from ..dplyr import distinct, bind_cols, group_vars, group_by_drop_default

from .chop import unchop, _vec_split
from .pack import unpack


@register_verb(DataFrame, context=Context.SELECT)
def nest(
    _data: DataFrame,
    _names_sep: str = None,
    base0_: bool = None,
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
        base0_: Whether `**cols` are 0-based
            if not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        Nested data frame.
    """
    if not cols:
        raise ValueError("`**cols` must not be empty.")

    all_columns = _data.columns
    colgroups = {}
    usedcols = set()
    for group, columns in cols.items():
        old_cols = all_columns[vars_select(all_columns, columns, base0=base0_)]
        usedcols = usedcols.union(old_cols)
        newcols = (
            old_cols
            if _names_sep is None
            else _strip_names(old_cols, group, _names_sep)
        )
        colgroups[group] = dict(zip(newcols, old_cols))

    asis = setdiff(_data.columns, usedcols)
    keys = _data[asis]
    u_keys = distinct(keys)

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

        nested.append(val)

    out = pandas.concat(nested, ignore_index=True, axis=1)
    out.columns = list(colgroups)
    if u_keys.shape[1] == 0:
        return out if isinstance(out, DataFrame) else out.to_frame()
    return u_keys >> bind_cols(recycle_value(out, u_keys.shape[0]))


@nest.register(DataFrameGroupBy, context=Context.SELECT)
def _(
    _data: DataFrameGroupBy,
    _names_sep: str = None,
    base0_: bool = None,
    **cols: Mapping[str, Union[str, int]],
) -> DataFrameGroupBy:
    """Nesting grouped dataframe"""
    if not cols:
        cols = {"data": setdiff(_data.columns, group_vars(_data))}
    out = nest.dispatch(DataFrame)(
        _data, **cols, _names_sep=_names_sep, base0_=base0_
    )
    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame, context=Context.SELECT)
def unnest(
    data: DataFrame,
    *cols: Union[str, int],
    keep_empty: bool = False,
    ptype: Union[Dtype, Mapping[str, Dtype]] = None,
    names_sep: str = None,
    names_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
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
        ptype: Providing the dtypes for the output columns.
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
        base0_: Whether `cols` are 0-based
            if not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        Data frame with selected columns unnested.
    """
    if not cols:
        raise ValueError("`*cols` is required when using unnest().")

    all_columns = data.columns
    cols = vars_select(all_columns, cols, base0=base0_)
    cols = all_columns[cols]

    out = data.copy()
    for col in cols:
        out[col] = _as_df(data[col])

    out = unchop(out, cols, keep_empty=keep_empty, ptype=ptype, base0_=base0_)
    return unpack(out, cols, names_sep=names_sep, names_repair=names_repair)


@unnest.register(DataFrameRowwise, context=Context.SELECT)
def _(
    data: DataFrameRowwise,
    *cols: Union[str, int],
    keep_empty: bool = False,
    ptype: Union[Dtype, Mapping[str, Dtype]] = None,
    names_sep: str = None,
    names_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
) -> DataFrame:
    """Unnest rowwise dataframe"""
    out = unnest.dispatch(DataFrame)(
        data,
        *cols,
        keep_empty=keep_empty,
        ptype=ptype,
        names_sep=names_sep,
        names_repair=names_repair,
        base0_=base0_,
    )
    return DataFrameGroupBy(
        out,
        _group_vars=group_vars(data),
        _group_drop=group_by_drop_default(data),
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
        elif is_scalar(val) and pandas.isnull(val):
            out.append(val)
        else:
            out.append(to_df(val, name=series.name))
    return out
