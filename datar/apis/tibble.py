from __future__ import annotations as _
from typing import Callable as _Callable

from pipda import (
    register_verb as _register_verb,
    register_func as _register_func,
)

from ..core.utils import (
    NotImplementedByCurrentBackendError as _NotImplementedByCurrentBackendError,
)


@_register_func(plain=True)
def tibble(
    *args,
    _name_repair: str | _Callable = "check_unique",
    _rows: int = None,
    _dtypes=None,
    _drop_index: bool = False,
    _index=None,
    **kwargs,
):
    """Constructs a data frame

    Args:
        *args: and
        **kwargs: A set of name-value pairs.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        _rows: Number of rows of a 0-col dataframe when args and kwargs are
            not provided. When args or kwargs are provided, this is ignored.
        _dtypes: The dtypes for each columns to convert to.
        _drop_index: Whether drop the index for the final data frame
        _index: The new index of the output frame

    Returns:
        A constructed tibble
    """
    raise _NotImplementedByCurrentBackendError("tibble")


@_register_func(pipeable=True, dispatchable=True)
def tibble_(
    *args,
    _name_repair: str | _Callable = "check_unique",
    _rows: int = None,
    _dtypes=None,
    _drop_index: bool = False,
    _index=None,
    **kwargs,
):
    raise _NotImplementedByCurrentBackendError("tibble_")


@_register_func(plain=True)
def tribble(
    *dummies,
    _name_repair: str | _Callable = "minimal",
    _dtypes=None,
):
    """Create dataframe using an easier to read row-by-row layout
    Unlike original API that uses formula (`f.col`) to indicate the column
    names, we use `f.col` to indicate them.

    Args:
        *dummies: Arguments specifying the structure of a dataframe
            Variable names should be specified with `f.name`
        _dtypes: The dtypes for each columns to convert to.

    Examples:
        >>> tribble(
        >>>     f.colA, f.colB,
        >>>     "a",    1,
        >>>     "b",    2,
        >>>     "c",    3,
        >>> )

    Returns:
        A dataframe
    """
    raise _NotImplementedByCurrentBackendError("tribble")


@_register_func(plain=True)
def tibble_row(
    *args,
    _name_repair: str | _Callable = "check_unique",
    _dtypes=None,
    **kwargs,
):
    """Constructs a data frame that is guaranteed to occupy one row.
    Scalar values will be wrapped with `[]`
    Args:
        *args: and
        **kwargs: A set of name-value pairs.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
    Returns:
        A constructed dataframe
    """
    raise _NotImplementedByCurrentBackendError("tibble_row")


@_register_verb()
def as_tibble(df):
    """Convert a DataFrame object to Tibble object"""
    raise _NotImplementedByCurrentBackendError("as_tibble", df)


@_register_verb()
def enframe(x, name="name", value="value"):
    """Converts mappings or lists to one- or two-column data frames.

    Args:
        x: a list, a dictionary or a dataframe with one or two columns
        name: and
        value: value Names of the columns that store the names and values.
            If `None`, a one-column dataframe is returned.
            `value` cannot be `None`

    Returns:
        A data frame with two columns if `name` is not None (default) or
        one-column otherwise.
    """
    raise _NotImplementedByCurrentBackendError("enframe", x)


@_register_verb()
def deframe(x):
    """Converts two-column data frames to a dictionary
    using the first column as name and the second column as value.
    If the input has only one column, a list.

    Args:
        x: A data frame.

    Returns:
        A dictionary or a list if only one column in the data frame.
    """
    raise _NotImplementedByCurrentBackendError("deframe", x)


@_register_verb()
def add_row(
    _data,
    *args,
    _before=None,
    _after=None,
    **kwargs,
):
    """Add one or more rows of data to an existing data frame.

    Aliases `add_case`

    Args:
        _data: Data frame to append to.
        *args: and
        **kwargs: Name-value pairs to add to the data frame.
        _before: and
        _after: row index where to add the new rows.
            (default to add after the last row)

    Returns:
        The dataframe with the added rows

    """
    raise _NotImplementedByCurrentBackendError("add_row", _data)


@_register_verb()
def add_column(
    _data,
    *args,
    _before=None,
    _after=None,
    _name_repair="check_unique",
    _dtypes=None,
    **kwargs,
):
    """Add one or more columns to an existing data frame.

    Args:
        _data: Data frame to append to
        *args: and
        **kwargs: Name-value pairs to add to the data frame
        _before: and
        _after: Column index or name where to add the new columns
            (default to add after the last column)
        _dtypes: The dtypes for the new columns, either a uniform dtype or a
            dict of dtypes with keys the column names

    Returns:
        The dataframe with the added columns
    """
    raise _NotImplementedByCurrentBackendError("add_column", _data)


@_register_verb()
def has_rownames(_data):
    """Detect if a data frame has row names

    Aliases `has_index`

    Args:
        _data: The data frame to check

    Returns:
        True if the data frame has index otherwise False.

    """
    raise _NotImplementedByCurrentBackendError("has_rownames", _data)


@_register_verb()
def remove_rownames(_data):
    """Remove the index/rownames of a data frame

    Aliases `remove_index`, `drop_index`, `remove_rownames`

    Args:
        _data: The data frame

    Returns:
        The data frame with index removed

    """
    raise _NotImplementedByCurrentBackendError("remove_rownames", _data)


@_register_verb()
def rownames_to_column(_data, var="rowname"):
    """Add rownames as a column

    Aliases `index_to_column`

    Args:
        _data: The data frame
        var: The name of the column

    Returns:
        The data frame with rownames added as one column. Note that the
        original index is removed.
    """
    raise _NotImplementedByCurrentBackendError("rownames_to_column", _data)


@_register_verb()
def rowid_to_column(_data, var="rowid"):
    """Add rownames as a column

    Args:
        _data: The data frame
        var: The name of the column

    Returns:
        The data frame with row ids added as one column.

    """
    raise _NotImplementedByCurrentBackendError("rowid_to_column", _data)


@_register_verb()
def column_to_rownames(_data, var="rowname"):
    """Set rownames/index with one column, and remove it

    Aliases `column_to_index`

    Args:
        _data: The data frame
        var: The column to conver to the rownames

    Returns:
        The data frame with the column converted to rownames
    """
    raise _NotImplementedByCurrentBackendError("column_to_rownames", _data)


# aliases
add_case = add_row
has_index = has_rownames
remove_index = drop_index = remove_rownames
index_to_column = rownames_to_column
column_to_index = column_to_rownames
