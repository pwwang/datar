"""Chop and unchop

https://github.com/tidyverse/tidyr/blob/master/R/chop.R
"""
from collections import defaultdict
from typing import Iterable

import numpy
import pandas
from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar
from pipda import register_verb

from ..core.utils import vars_select, apply_dtypes
from ..core.contexts import Context
from ..core.tibble import reconstruct_tibble

from ..base import union, setdiff, NA
from ..dplyr import ungroup

from .drop_na import drop_na


def _keep_column_order(df: DataFrame, order: Iterable[str]):
    """Keep the order of columns as given `order`
    We cannot do `df[order]` directly, since `df` may have nested df columns.
    """
    out_columns = []
    for col in order:
        if col in df:
            out_columns.append(col)
        else:
            out_columns.extend(
                (dfcol for dfcol in df.columns if dfcol.startswith(f"{col}$"))
            )
    if set(out_columns) != set(df.columns):  # pragma: no cover
        raise ValueError("Given `order` does not select all columns.")

    return df[out_columns]


@register_verb(DataFrame, context=Context.SELECT)
def chop(
    data: DataFrame,
    cols=None,
) -> DataFrame:
    """Makes data frame shorter by converting rows within each group
    into list-columns.

    Args:
        data: A data frame
        cols: Columns to chop

    Returns:
        Data frame with selected columns chopped
    """
    if cols is None or len(list(cols)) == 0:
        return data.copy()

    all_columns = data.columns
    cols = vars_select(all_columns, cols)
    cols = all_columns[cols]
    # when cols is empty
    # order may change for all_columns.difference([])
    key_cols = (
        setdiff(all_columns, cols, __ast_fallback="normal")
        if cols.size > 0
        else all_columns
    )
    ungrouped = ungroup(data, __ast_fallback="normal")
    if key_cols.size == 0:
        grouped = ungrouped.groupby([1] * data.shape[0], sort=False)
        out = grouped.agg(list).reset_index(drop=True)
    else:
        grouped = ungroup(data, __ast_fallback="normal").groupby(
            list(key_cols),
            dropna=False,
            observed=True,
            sort=False,
        )
        out = grouped.agg(list).reset_index()
    return reconstruct_tibble(data, out)


@register_verb(DataFrame, context=Context.SELECT)
def unchop(
    data: DataFrame,
    cols=None,
    keep_empty: bool = False,
    dtypes=None,
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
        dtypes: Providing the dtypes for the output columns.
            Could be a single dtype, which will be applied to all columns, or
            a dictionary of dtypes with keys for the columns and values the
            dtypes.
            For nested data frames, we need to specify `col$a` as key. If `col`
            is used as key, all columns of the nested data frames will be casted
            into that dtype.

    Returns:
        A data frame with selected columns unchopped.
    """
    # TODO: use df.explode() with pandas 1.3+?
    all_columns = data.columns
    cols = vars_select(all_columns, cols)

    if len(cols) == 0 or data.shape[0] == 0:
        return data.copy()

    cols = all_columns[cols]
    key_cols = all_columns.difference(cols).tolist()
    out = _unchopping(
        ungroup(data, __ast_fallback="normal"),
        cols,
        key_cols,
        keep_empty,
    )

    apply_dtypes(out, dtypes)
    return reconstruct_tibble(data, out)


def _unchopping(
    data: DataFrame,
    data_cols: Iterable[str],
    key_cols: Iterable[str],
    keep_empty: bool,
) -> DataFrame:
    """Unchop the data frame

    See https://stackoverflow.com/questions/53218931/how-to-unnest-explode-a-column-in-a-pandas-dataframe
    """  # noqa: E501
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
    out = _keep_column_order(DataFrame(key_data), data.columns)
    if not keep_empty:
        out = drop_na(
            out,
            *val_data,
            how_="all",
            __ast_fallback="normal",
        )
    apply_dtypes(out, dtypes)
    return out


def _unchopping_df_column(
    series: Series,
):
    """Unchopping dataframe column"""
    # Get union column names
    union_cols = []
    # try to keep the same dtype
    dtypes = None
    for val in series:
        if isinstance(val, DataFrame):
            union_cols = union(
                union_cols,
                val.columns,
                __ast_fallback="normal",
            )
            if dtypes is None:
                dtypes = {col: val[col].dtype for col in val}
            else:
                for col in val:
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


def _unchopping_nondf_column(series: Series):
    """Unchopping non-dataframe column"""
    val_data = {}
    vals = [
        [val]
        if is_scalar(val)
        else val
        for val in series
    ]

    val_data[series.name] = Series(
        numpy.concatenate(
            vals,
            axis=None,
            # casting="no" # only for numpy 1.20.0+
        ),
        dtype=series.dtype if series.dtype is not None else object,
    )
    return val_data, [len(val) for val in vals], {}
