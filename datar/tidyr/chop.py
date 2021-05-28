"""Chop and unchop

https://github.com/tidyverse/tidyr/blob/master/R/chop.R
"""
from typing import Mapping, Optional, Union, Iterable

import pandas
from pandas import DataFrame, Series
from pipda import register_verb

from ..core.types import IntOrIter, StringOrIter
from ..core.utils import vars_select, copy_attrs, apply_dtypes
from ..core.exceptions import ColumnNotExistingError
from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy

from ..dplyr import (
    bind_cols, group_by, mutate, pull,
    group_data, group_by_drop_default, group_vars
)

from .verbs import drop_na

@register_verb(DataFrame, context=Context.SELECT)
def chop(
        data: DataFrame,
        cols: Optional[Union[IntOrIter, StringOrIter]] = None,
        _base0: Optional[bool] = None
) -> DataFrame:
    """Makes data frame shorter by converting rows within each group
    into list-columns.

    Args:
        data: A data frame
        cols: Columns to chop
        _base0: Whether `cols` are 0-based
            if not provided, will use `datar.base.getOption('index.base.0')`

    Returns:
        Data frame with selected columns chopped
    """
    if cols is None:
        return data.copy()

    all_columns = data.columns
    cols = vars_select(all_columns, cols, base0=_base0)
    cols = all_columns[cols]
    # when cols is empty
    # order may change for all_columns.difference([])
    key_cols = all_columns.difference(cols) if len(cols) > 0 else all_columns

    vals = data[cols]
    keys = data[key_cols]
    split = _vec_split(vals, keys)

    try:
        split_key = split >> pull('key', to='frame')
    except ColumnNotExistingError:
        split_key = None
    split_val = split >> pull('val', to='list')

    compacted = []
    for val in split_val:
        compacted.append(_compact_df(val))
    if not compacted:
        vals = DataFrame(columns=cols)
    else:
        vals = pandas.concat(compacted, ignore_index=True)

    out = bind_cols(split_key, vals)
    if isinstance(data, DataFrameGroupBy):
        out = data.__class__(
            out,
            _group_vars=group_vars(data),
            _drop=group_by_drop_default(data)
        )

    copy_attrs(out, data)
    return out

@register_verb(DataFrame, context=Context.SELECT)
def unchop(
        data: DataFrame,
        cols: Optional[Union[IntOrIter, StringOrIter]] = None,
        keep_empty: bool = False,
        dtypes: Optional[
            Mapping[str, Union[StringOrIter, type, Iterable[type]]]
        ] = None,
        _base0: Optional[bool] = None
) -> DataFrame:
    """Makes df longer by expanding list-columns so that each element
    of the list-column gets its own row in the output.

    Recycling size-1 elements might be different from `tidyr`
        >>> df = tibble(x=[1, [2,3]], y=[[2,3], 1])
        >>> df >> unchop([f.x, f.y])
        >>> # tibble(x=[1,2,3], y=[2,3,1])
        >>> # instead of following in tidyr
        >>> # tibble(x=[1,1,2,3], y=[2,3,1,1])

    Args:
        data: A data frame.
        cols: Columns to unchop.
        keep_empty: By default, you get one row of output for each element
            of the list your unchopping/unnesting.
            This means that if there's a size-0 element
            (like NULL or an empty data frame), that entire row will be
            dropped from the output.
            If you want to preserve all rows, use `keep_empty` = `True` to
            replace size-0 elements with a single row of missing values.
        dtypes: NOT `ptype`. Providing the dtypes for the output columns.
            Could be a single dtype, which will be applied to all columns, or
            a dictionary of dtypes with keys for the columns and values the
            dtypes.
        _base0: Whether `cols` are 0-based
            if not provided, will use `datar.base.getOption('index.base.0')`

    Returns:
        A data frame with selected columns unchopped.
    """
    all_columns = data.columns
    cols = vars_select(all_columns, cols, base0=_base0)

    if len(cols) == 0:
        return data.copy()

    cols = all_columns[cols]
    key_cols = all_columns.difference(cols).tolist()
    if key_cols:
        out = data.set_index(key_cols).apply(Series.explode)
    else:
        out = data.apply(Series.explode, ignore_index=True)
    if not keep_empty:
        out = drop_na(out, how='all')

    # keep order
    out = out.reset_index()[all_columns]
    apply_dtypes(out, dtypes)

    if isinstance(data, DataFrameGroupBy):
        out = data.__class__(
            out,
            _group_vars=group_vars(data),
            _drop=group_by_drop_default(data)
        )

    copy_attrs(out, data)
    return out

def _vec_split(
        x: Union[DataFrame, Series],
        by: Union[DataFrame, Series]
) -> DataFrame:
    """Split a vector into groups

    Returns a data frame with columns `key` and `val`. `key` is a stacked column
    with data from by.
    """
    df = bind_cols(x, by) >> group_by(*by.columns)
    gdata = group_data(df)
    out = DataFrame(index=gdata.index)
    return mutate(
        out,
        key=gdata[by.columns],
        val=[x.iloc[rows, :] for rows in gdata._rows]
    )

def _compact_df(data: DataFrame) -> DataFrame:
    """Compact each series as list in a data frame"""
    out = DataFrame(index=[0], columns=data.columns)
    for col in data.columns:
        out.loc[0, col] = data[col].values.tolist()
    return out
