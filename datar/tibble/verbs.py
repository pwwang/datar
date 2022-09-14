"""Functions ported from tidyverse-tibble"""
from ..core.backends.pandas import DataFrame, Series, RangeIndex
from ..core.backends.pandas.api.types import is_scalar
from pipda import Context, register_verb

from ..core.utils import logger
from ..core.broadcast import broadcast_to
from ..core.tibble import (
    TibbleGrouped,
    TibbleRowwise,
    Tibble,
    reconstruct_tibble,
)
from ..base import setdiff

from .tibble import tibble


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
    if not value:
        raise ValueError("`value` can't be empty.")

    if x is None:
        x = []
    if is_scalar(x):
        x = [x]

    if len(getattr(x, "shape", ())) > 1:
        raise ValueError(
            f"`x` must not have more than one dimension, got {len(x.shape)}."
        )

    if not name and isinstance(x, dict):
        x = x.values()

    elif name:
        if not isinstance(x, dict):
            names = range(len(x))
            values = x
        else:
            names = x.keys()
            values = x.values()
        x = (list(item) for item in zip(names, values))

    return Tibble(x, columns=[name, value] if name else [value])


def deframe(x):
    """Converts two-column data frames to a dictionary
    using the first column as name and the second column as value.
    If the input has only one column, a list.

    Args:
        x: A data frame.

    Returns:
        A dictionary or a list if only one column in the data frame.
    """
    if x.shape[1] == 1:
        return x.iloc[:, 0].values

    if x.shape[1] != 2:
        logger.warning(
            "`x` must be a one- or two-column data frame in `deframe()`."
        )

    return dict(zip(x.iloc[:, 0], x.iloc[:, 1]))


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"_before": Context.SELECT, "_after": Context.SELECT},
)
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
    if isinstance(_data, TibbleGrouped) and not isinstance(
        _data, TibbleRowwise
    ):
        raise ValueError("Can't add rows to grouped data frames.")

    if not args and not kwargs:
        df = DataFrame(index=[0], columns=_data.columns)
    else:
        df = tibble(*args, **kwargs)
        if df.shape[0] == 0:
            for col in _data.columns:
                df[col] = Series(dtype=_data[col].dtype)

    extra_vars = setdiff(df.columns, _data.columns, __ast_fallback="normal")
    if extra_vars.size > 0:
        raise ValueError(f"New rows can't add columns: {extra_vars}")

    pos = _pos_from_before_after(_before, _after, _data.shape[0])
    out = _rbind_at(_data, df, pos)

    if isinstance(_data, TibbleRowwise):
        out = reconstruct_tibble(_data, out)

    return out


add_case = add_row


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"_before": Context.SELECT, "_after": Context.SELECT},
)
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
    df = tibble(*args, **kwargs, _name_repair="minimal", _dtypes=_dtypes)

    if df.shape[1] == 0:
        return _data.copy()

    grouper = None
    if isinstance(_data, TibbleGrouped):
        grouper = _data._datar["grouped"].grouper
    df = broadcast_to(df, _data.index, grouper)
    pos = _pos_from_before_after_names(_before, _after, _data.columns.tolist())

    out = _cbind_at(_data, df, pos, _name_repair)
    if len(_data) == 0:
        out = out.loc[[], :]
    return reconstruct_tibble(_data, out)


@register_verb(DataFrame)
def has_rownames(_data):
    """Detect if a data frame has row names

    Aliases `has_index`

    Args:
        _data: The data frame to check

    Returns:
        True if the data frame has index otherwise False.

    """
    return not (
        isinstance(_data.index, RangeIndex)
        and _data.index.start == 0
        and _data.index.step == 1
    )


has_index = has_rownames


@register_verb(DataFrame)
def remove_rownames(_data):
    """Remove the index/rownames of a data frame

    Aliases `remove_index`, `drop_index`, `remove_rownames`

    Args:
        _data: The data frame

    Returns:
        The data frame with index removed

    """
    return _data.reset_index(drop=True)


remove_index = drop_index = remove_rownames


@register_verb(DataFrame, context=Context.SELECT)
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
    if var in _data.columns:
        raise ValueError(f"Column name `{var}` must not be duplicated.")

    from ..dplyr.mutate import mutate

    return remove_rownames(
        mutate(
            _data,
            **{var: _data.index},
            _before=1,
            __ast_fallback="normal",
        ),
        __ast_fallback="normal",
    )


index_to_column = rownames_to_column


@register_verb(DataFrame, context=Context.SELECT)
def rowid_to_column(_data, var="rowid"):
    """Add rownames as a column

    Args:
        _data: The data frame
        var: The name of the column

    Returns:
        The data frame with row ids added as one column.

    """
    if var in _data.columns:
        raise ValueError(f"Column name `{var}` must not be duplicated.")

    from ..dplyr.mutate import mutate

    return remove_rownames(
        mutate(
            _data,
            **{var: range(_data.shape[0])},
            _before=1,
            __ast_fallback="normal",
        ),
        __ast_fallback="normal",
    )


@register_verb(DataFrame, context=Context.SELECT)
def column_to_rownames(_data, var="rowname"):
    """Set rownames/index with one column, and remove it

    Aliases `column_to_index`

    Args:
        _data: The data frame
        var: The column to conver to the rownames

    Returns:
        The data frame with the column converted to rownames
    """
    if has_rownames(_data, __ast_fallback="normal"):
        raise ValueError("`_data` must be a data frame without row names.")

    from ..dplyr.mutate import mutate

    try:
        rownames = [str(name) for name in _data[var]]
    except KeyError:
        raise KeyError(f"Column `{var}` does not exist.") from None
    out = mutate(_data, __ast_fallback="normal", **{var: None})
    out.index = rownames
    return out


column_to_index = column_to_rownames

# Helpers ------------------------------------------------------------------


def _pos_from_before_after_names(before, after, names) -> int:
    """Get the position to insert from before and after"""
    if before is not None:
        before = _check_names_before_after(before, names)
    if after is not None:
        after = _check_names_before_after(after, names)
    return _pos_from_before_after(before, after, len(names))


def _check_names_before_after(pos, names) -> int:
    """Get position by given index or name"""
    if not isinstance(pos, str):
        return pos
    try:
        return names.index(pos)
    except ValueError:
        raise KeyError(f"Column `{pos}` does not exist.") from None


def _cbind_at(data, df, pos: int, _name_repair):
    """Column bind at certain pos, 0-based"""
    from ..dplyr import bind_cols

    part1 = data.iloc[:, :pos]
    part2 = data.iloc[:, pos:]
    return bind_cols(
        part1,
        df,
        part2,
        _name_repair=_name_repair,
        __ast_fallback="normal",
    )


def _pos_from_before_after(before, after, length):
    """Get the position to insert from before and after"""
    if before is not None and after is not None:
        raise ValueError("Can't specify both `_before` and `_after`.")

    if before is None and after is None:
        return length

    if after is not None:
        return after + 1
    return before


def _rbind_at(data, df, pos):
    """Row bind at certain pos, 0-based"""
    from ..dplyr import bind_rows

    part1 = data.iloc[:pos, :]
    part2 = data.iloc[pos:, :]
    return bind_rows(part1, df, part2, __ast_fallback="normal")
