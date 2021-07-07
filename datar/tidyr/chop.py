"""Chop and unchop

https://github.com/tidyverse/tidyr/blob/master/R/chop.R
"""
from collections import defaultdict
from typing import Iterable, List, Mapping, Tuple, Union

import numpy
import pandas
from pandas import DataFrame, Series
from pipda import register_verb

from ..core.types import IntOrIter, StringOrIter, Dtype, is_scalar
from ..core.utils import (
    df_getitem,
    df_setitem,
    vars_select,
    copy_attrs,
    apply_dtypes,
    keep_column_order,
    reconstruct_tibble,
)
from ..core.exceptions import ColumnNotExistingError
from ..core.contexts import Context

from ..base import union, NA
from ..dplyr import bind_cols, group_by, arrange, group_data

from .drop_na import drop_na


@register_verb(DataFrame, context=Context.SELECT)
def chop(
    data: DataFrame,
    cols: Union[IntOrIter, StringOrIter] = None,
    base0_: bool = None,
) -> DataFrame:
    """Makes data frame shorter by converting rows within each group
    into list-columns.

    Args:
        data: A data frame
        cols: Columns to chop
        base0_: Whether `cols` are 0-based
            if not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        Data frame with selected columns chopped
    """
    if cols is None:
        return data.copy()

    all_columns = data.columns
    cols = vars_select(all_columns, cols, base0=base0_)
    cols = all_columns[cols]
    # when cols is empty
    # order may change for all_columns.difference([])
    key_cols = all_columns.difference(cols) if len(cols) > 0 else all_columns

    vals = data[cols]
    keys = data[key_cols]

    compacted = []
    if data.shape[0] == 0:
        split_key = keys
    else:
        split = _vec_split(vals, keys)
        try:
            split_key = df_getitem(split, "key")
        except (KeyError, ColumnNotExistingError):
            split_key = None
        split_val = df_getitem(split, "val")

        for val in split_val:
            compacted.append(_compact_df(val))

    if not compacted:
        vals = DataFrame(columns=cols)
    else:
        vals = pandas.concat(compacted, ignore_index=True)

    out = split_key >> bind_cols(vals)
    return reconstruct_tibble(data, out, keep_rowwise=True)


@register_verb(DataFrame, context=Context.SELECT)
def unchop(
    data: DataFrame,
    cols: Union[IntOrIter, StringOrIter] = None,
    keep_empty: bool = False,
    ptype: Union[Dtype, Mapping[str, Dtype]] = None,
    base0_: bool = None,
) -> DataFrame:
    """Makes df longer by expanding list-columns so that each element
    of the list-column gets its own row in the output.

    See https://tidyr.tidyverse.org/reference/chop.html

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
        ptype: Providing the dtypes for the output columns.
            Could be a single dtype, which will be applied to all columns, or
            a dictionary of dtypes with keys for the columns and values the
            dtypes.
            For nested data frames, we need to specify `col$a` as key. If `col`
            is used as key, all columns of the nested data frames will be casted
            into that dtype.
        base0_: Whether `cols` are 0-based
            if not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        A data frame with selected columns unchopped.
    """
    all_columns = data.columns
    cols = vars_select(all_columns, cols, base0=base0_)

    if len(cols) == 0 or data.shape[0] == 0:
        return data.copy()

    cols = all_columns[cols]
    key_cols = all_columns.difference(cols).tolist()
    out = _unchopping(data, cols, key_cols, keep_empty)

    apply_dtypes(out, ptype)
    return reconstruct_tibble(data, out, keep_rowwise=True)


def _vec_split(
    x: Union[DataFrame, Series], by: Union[DataFrame, Series]
) -> DataFrame:
    """Split a vector into groups

    Returns a data frame with columns `key` and `val`. `key` is a stacked column
    with data from by.
    """
    if isinstance(x, Series):  # pragma: no cover, always a data frame?
        x = x.to_frame()
    if isinstance(by, Series):  # pragma: no cover, always a data frame?
        by = by.to_frame()

    df = x >> bind_cols(by)
    if df.shape[0] == 0:
        return DataFrame(columns=["key", "val"])
    df = df >> group_by(*by.columns)
    gdata = group_data(df)
    gdata = arrange(gdata, gdata._rows)
    out = DataFrame(index=gdata.index)
    out = df_setitem(out, "key", gdata[by.columns])
    return df_setitem(out, "val", [x.iloc[rows, :] for rows in gdata._rows])


def _compact_df(data: DataFrame) -> DataFrame:
    """Compact each series as list in a data frame"""
    out = DataFrame(index=[0], columns=data.columns)
    for col in data.columns:
        out.loc[0, col] = data[col].values.tolist()
    return out


def _unchopping(
    data: DataFrame,
    data_cols: Iterable[str],
    key_cols: Iterable[str],
    keep_empty: bool,
) -> DataFrame:
    # pylint: disable=line-too-long
    """Unchop the data frame

    See https://stackoverflow.com/questions/53218931/how-to-unnest-explode-a-column-in-a-pandas-dataframe
    """
    # pylint: enable=line-too-long
    # key_cols could be empty
    rsize = None
    val_data = {}
    for dcol in data_cols:
        # check dtype first so that we don't need to check
        # other types of columns element by element
        is_df_col = data_cols.dtype == object and all(
            # it's either null or a dataframe
            (is_scalar(val) and pandas.isnull(val))
            or isinstance(val, DataFrame)
            for val in data[dcol]
        )
        if is_df_col:
            vdata, sizes, dtypes = _unchopping_df_column(data[dcol])
        else:
            vdata, sizes, dtypes = _unchopping_nondf_column(data[dcol])
        val_data.update(vdata)

        if rsize is None:
            rsize = sizes
        else:
            tmpsize = []
            for prevsize, cursize in zip(rsize, sizes):
                if prevsize != cursize and 1 not in (prevsize, cursize):
                    raise ValueError(
                        f"Incompatible lengths: {prevsize}, {cursize}."
                    )
                tmpsize.append(max(prevsize, cursize))
            rsize = tmpsize

    key_data = {key: numpy.repeat(data[key].values, rsize) for key in key_cols}
    key_data.update(val_data)
    # DataFrame(key_data) may have nested dfs
    # say y$a, then ['y'] will not select it
    out = keep_column_order(DataFrame(key_data), data.columns)
    if not keep_empty:
        out = drop_na(out, *val_data, how_="all")
    apply_dtypes(out, dtypes)
    copy_attrs(out, data)
    return out


def _unchopping_df_column(
    series: Series,
) -> Tuple[Mapping[str, List], List[int], Mapping[str, Dtype]]:
    """Unchopping dataframe column"""
    # Get union column names
    union_cols = []
    # try to keep the same dtype
    dtypes = None
    for val in series:
        if isinstance(val, DataFrame):
            union_cols = union(union_cols, val.columns)
            if dtypes is None:
                dtypes = {col: val[col].dtype for col in val}
            else:
                for col in val:
                    # pylint: disable=unsupported-membership-test
                    # pylint: disable=unsupported-delete-operation
                    # TODO: test
                    if col in dtypes and dtypes[col] != val[col].dtype:
                        del dtypes[col]  # pragma: no cover

    sizes = []
    val_data = defaultdict(list)
    # add missing columns to each df
    for val in series:
        if isinstance(val, DataFrame):
            for col in union_cols:
                val_data[f"{series.name}${col}"].extend(
                    val[col] if col in val else [NA] * val.shape[0]
                )
            sizes.append(val.shape[0])
        else:  # null
            for col in union_cols:
                val_data[f"{series.name}${col}"].append(NA)
            sizes.append(1)

    return val_data, sizes, dtypes


def _unchopping_nondf_column(
    series: Series,
) -> Tuple[Mapping[str, List], List[int], Mapping[str, Dtype]]:
    """Unchopping non-dataframe column"""
    val_data = {}
    vals = [[val] if is_scalar(val) else val for val in series]
    val_data[series.name] = Series(
        numpy.concatenate(
            vals,
            axis=None,
            # casting="no" # only for numpy 1.20.0+
        ),
        dtype=series.dtype,
    )
    return val_data, [len(val) for val in vals], {}
