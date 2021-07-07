"""Provide functions to manipulate multiple rows

https://github.com/tidyverse/dplyr/blob/master/R/rows.R
"""
from typing import List

import numpy
from pandas import DataFrame
from pipda import register_verb

from ..base import setdiff
from ..core.types import StringOrIter, is_scalar, is_null, is_not_null
from ..core.utils import logger
from ..tibble import rownames_to_column

from .bind import bind_rows
from .join import left_join
from .funs import coalesce


@register_verb(DataFrame)
def rows_insert(
    x: DataFrame,
    y: DataFrame,
    by: StringOrIter = None,
    copy: bool = True,
) -> DataFrame:
    """Adds new rows to a data frame

    Argument `in_place` not supported, as we always do data frames here.

    Args:
        x: The seed data frame
        y: The data frame with rows to be inserted into `x`.
            - Key values in `y` must not occur in `x`
            - `y` must have the same or a subset columns of `x`
        by: A string or a list of strings giving the key columns.
            The key values must uniquely identify each row
            (i.e. each combination of key values occurs at most once),
            and the key columns must exist in both x and y.
            By default, we use the first column in y, since the first column
            is a reasonable place to put an identifier variable.
        copy: If `False`, do not copy data unnecessarily.
            Original API does not support this. This argument will be
            passed by to `pandas.concat()` as `copy` argument.

    Returns:
        A data frame with `y` inserted into `x`
    """
    key = _rows_check_key(by, x, y)
    _rows_check_key_df(x, key, df_name="x")
    _rows_check_key_df(y, key, df_name="y")

    idx = _rows_match(y[key], x[key])
    bad = is_not_null(idx)
    if any(bad):
        raise ValueError("Attempting to insert duplicate rows.")

    return bind_rows(x, y, _copy=copy)


@register_verb(DataFrame)
def rows_update(
    x: DataFrame,
    y: DataFrame,
    by: StringOrIter = None,
    copy: bool = True,
) -> DataFrame:
    """Modifies existing rows in a data frame

    See Also:
        [`rows_insert`](datar.dplyr.rows.rows_insert)

    Args:
        x: The seed data frame
        y: The data frame with rows to be inserted into `x`.
            - Key values in `y` must not occur in `x`
            - `y` must have the same or a subset columns of `x`
        by: A string or a list of strings giving the key columns.
            The key values must uniquely identify each row
            (i.e. each combination of key values occurs at most once),
            and the key columns must exist in both x and y.
            By default, we use the first column in y, since the first column
            is a reasonable place to put an identifier variable.
        copy: Whether `x` should be copied and updated or updated directly

    Returns:
        `x` with values of keys updated
    """
    key = _rows_check_key(by, x, y)
    _rows_check_key_df(x, key, df_name="x")
    _rows_check_key_df(y, key, df_name="y")

    idx = _rows_match(y[key], x[key])
    bad = is_null(idx)
    if any(bad):
        raise ValueError("Attempting to update missing rows.")

    idx = idx.astype(int)
    if copy:
        x = x.copy()
    x.loc[idx, y.columns] = y.values
    return x


@register_verb(DataFrame)
def rows_patch(
    x: DataFrame,
    y: DataFrame,
    by: StringOrIter = None,
    copy: bool = True,
) -> DataFrame:
    """Works like `rows_update()` but only overwrites `NA` values.

    See Also:
        [`rows_insert`](datar.dplyr.rows.rows_insert)

    Args:
        x: The seed data frame
        y: The data frame with rows to be inserted into `x`.
            - Key values in `y` must not occur in `x`
            - `y` must have the same or a subset columns of `x`
        by: A string or a list of strings giving the key columns.
            The key values must uniquely identify each row
            (i.e. each combination of key values occurs at most once),
            and the key columns must exist in both x and y.
            By default, we use the first column in y, since the first column
            is a reasonable place to put an identifier variable.
        copy: Whether `x` should be copied and updated or updated directly

    Returns:
        `x` with values of keys updated
    """
    key = _rows_check_key(by, x, y)
    _rows_check_key_df(x, key, df_name="x")
    _rows_check_key_df(y, key, df_name="y")

    idx = _rows_match(y[key], x[key])
    bad = is_null(idx)
    if any(bad):
        raise ValueError("Attempting to patch missing rows.")

    new_data = []
    for col in y.columns:
        new_data.append(coalesce(x.loc[idx, col].values, y[col]))

    if copy:
        x = x.copy()
    x.loc[idx, y.columns] = numpy.array(new_data).T
    return x


@register_verb(DataFrame)
def rows_upsert(
    x: DataFrame,
    y: DataFrame,
    by: StringOrIter = None,
    copy: bool = True,
) -> DataFrame:
    """Inserts or updates depending on whether or not the
    key value in `y` already exists in `x`.

    See Also:
        [`rows_insert`](datar.dplyr.rows.rows_insert)

    Args:
        x: The seed data frame
        y: The data frame with rows to be inserted into `x`.
            - Key values in `y` must not occur in `x`
            - `y` must have the same or a subset columns of `x`
        by: A string or a list of strings giving the key columns.
            The key values must uniquely identify each row
            (i.e. each combination of key values occurs at most once),
            and the key columns must exist in both x and y.
            By default, we use the first column in y, since the first column
            is a reasonable place to put an identifier variable.
        copy: If `False`, do not copy data unnecessarily.
            Original API does not support this. This argument will be
            passed by to `pandas.concat()` as `copy` argument.

    Returns:
        `x` with values of keys updated
    """
    key = _rows_check_key(by, x, y)
    _rows_check_key_df(x, key, df_name="x")
    _rows_check_key_df(y, key, df_name="y")

    idx = _rows_match(y[key], x[key])
    new = is_null(idx)
    # idx of x
    idx_existing = idx[~new]

    x.loc[idx_existing, y.columns] = y.loc[~new].values
    return bind_rows(x, y.loc[new], _copy=copy)


@register_verb(DataFrame)
def rows_delete(
    x: DataFrame,
    y: DataFrame,
    by: StringOrIter = None,
    copy: bool = True,
) -> DataFrame:
    """Deletes rows; key values in `y` must exist in `x`.

    See Also:
        [`rows_insert`](datar.dplyr.rows.rows_insert)

    Args:
        x: The seed data frame
        y: The data frame with rows to be inserted into `x`.
            - Key values in `y` must not occur in `x`
            - `y` must have the same or a subset columns of `x`
        by: A string or a list of strings giving the key columns.
            The key values must uniquely identify each row
            (i.e. each combination of key values occurs at most once),
            and the key columns must exist in both x and y.
            By default, we use the first column in y, since the first column
            is a reasonable place to put an identifier variable.
        copy: Whether `x` should be copied and deleted or deleted directly

    Returns:
        `x` with values of keys deleted
    """
    key = _rows_check_key(by, x, y)
    _rows_check_key_df(x, key, df_name="x")
    _rows_check_key_df(y, key, df_name="y")

    extra_cols = setdiff(y.columns, key)
    if len(extra_cols) > 0:
        logger.info("Ignoring extra columns: %s", extra_cols)

    idx = _rows_match(y[key], x[key])
    bad = is_null(idx)

    if any(bad):
        raise ValueError("Attempting to delete missing rows.")

    if copy:
        x = x.copy()

    return x.loc[~x.index.isin(idx), :]


# helpers -----------------------------------------------------------------


def _rows_check_key(by: StringOrIter, x: DataFrame, y: DataFrame) -> List[str]:
    """Check the key and return the valid key"""
    if by is None:
        by = y.columns[0]
        logger.info("Matching, by=%r", by)

    if is_scalar(by):
        by = [by] # type: ignore

    for by_elem in by:
        if not isinstance(by_elem, str):
            raise ValueError("`by` must be a string or a list of strings.")

    bad = setdiff(y.columns, x.columns)
    if len(bad) > 0:
        raise ValueError("All columns in `y` must exist in `x`.")

    return by


def _rows_check_key_df(df: DataFrame, by: List[str], df_name: str) -> None:
    """Check key with the data frame"""
    y_miss = setdiff(by, df.columns)
    if len(y_miss) > 0:
        raise ValueError(f"All `by` columns must exist in `{df_name}`.")

    if any(df.duplicated(by)):
        raise ValueError(f"`{df_name}` key values are not unique.")


def _rows_match(x: DataFrame, y: DataFrame) -> numpy.ndarray:
    """Mimic vctrs::vec_match"""
    id_col = "__id__"
    y_with_id = rownames_to_column(y, var=id_col)
    # pylint: disable=no-value-for-parameter
    return (x >> left_join(y_with_id))[id_col].values
