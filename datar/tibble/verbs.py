"""Functions ported from tidyverse-tibble"""
from typing import Any, Callable, Iterable, List, Mapping, Union

from pandas import DataFrame, Series, RangeIndex
from pipda import Context, register_verb

from ..core.utils import (
    copy_attrs,
    get_option,
    position_after,
    position_at,
    recycle_value,
    logger,
    reconstruct_tibble,
)
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.types import is_scalar, Dtype
from ..core.exceptions import ColumnNotExistingError
from ..base import setdiff

from .tibble import tibble


def enframe(
    x: Union[Iterable, Mapping],
    name: str = "name",
    value: str = "value",
    base0_: bool = None,
) -> DataFrame:
    """Converts mappings or lists to one- or two-column data frames.

    Args:
        x: a list, a dictionary or a dataframe with one or two columns
        name: and
        value: value Names of the columns that store the names and values.
            If `None`, a one-column dataframe is returned.
            `value` cannot be `None`
        base0_: Whether the indexes for lists converted to name are 0-based
            or not.

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
            base0_ = get_option("index.base.0", base0_)
            names = (i + int(not base0_) for i in range(len(x)))
            values = x
        else:
            names = x.keys()
            values = x.values()
        x = (list(item) for item in zip(names, values))

    return DataFrame(x, columns=[name, value] if name else [value])


def deframe(x: DataFrame) -> Union[Iterable, Mapping]:
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
    _data: DataFrame,
    *args: Any,
    _before: int = None,
    _after: int = None,
    base0_: bool = None,
    **kwargs: Any,
) -> DataFrame:
    """Add one or more rows of data to an existing data frame.

    Aliases `add_case`

    Args:
        _data: Data frame to append to.
        *args: and
        **kwargs: Name-value pairs to add to the data frame.
        _before: and
        _after: row index where to add the new rows.
            (default to add after the last row)
        base0_: Whether `_before` and `_after` are 0-based or not.

    Returns:
        The dataframe with the added rows

    """
    if isinstance(_data, DataFrameGroupBy) and not isinstance(
        _data, DataFrameRowwise
    ):
        raise ValueError("Can't add rows to grouped data frames.")

    if not args and not kwargs:
        df = DataFrame(index=[0], columns=_data.columns)
    else:
        df = tibble(*args, **kwargs, frame_=2)
        if df.shape[0] == 0:
            for col in _data.columns:
                df[col] = Series(dtype=_data[col].dtype)

    extra_vars = setdiff(df.columns, _data.columns)
    if extra_vars:
        raise ValueError(f"New rows can't add columns: {extra_vars}")

    pos = _pos_from_before_after(_before, _after, _data.shape[0], base0_)
    out = _rbind_at(_data, df, pos)

    if isinstance(_data, DataFrameRowwise):
        out = reconstruct_tibble(_data, out, keep_rowwise=True)
    else:
        copy_attrs(out, _data)
    return out


add_case = add_row  # pylint: disable=invalid-name


@register_verb(
    DataFrame,
    context=Context.EVAL,
    extra_contexts={"_before": Context.SELECT, "_after": Context.SELECT},
)
def add_column(
    _data: DataFrame,
    *args: Any,
    _before: Union[str, int] = None,
    _after: Union[str, int] = None,
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
    dtypes_: Union[Dtype, Mapping[str, Dtype]] = None,
    **kwargs: Any,
) -> DataFrame:
    """Add one or more columns to an existing data frame.

    Args:
        _data: Data frame to append to
        *args: and
        **kwargs: Name-value pairs to add to the data frame
        _before: and
        _after: Column index or name where to add the new columns
            (default to add after the last column)
        base0_: Whether `_before` and `_after` are 0-based if they are index.
            if not given, will be determined by `get_option('index_base_0')`,
            which is `False` by default.
        dtypes_: The dtypes for the new columns, either a uniform dtype or a
            dict of dtypes with keys the column names

    Returns:
        The dataframe with the added columns
    """
    df = tibble(
        *args, **kwargs, _name_repair="minimal", dtypes_=dtypes_, frame_=2
    )

    if df.shape[1] == 0:
        return _data.copy()

    df = recycle_value(df, len(_data), "new columns")
    pos = _pos_from_before_after_names(
        _before, _after, _data.columns.tolist(), base0_
    )

    out = _cbind_at(_data, df, pos, _name_repair)
    if len(_data) == 0:
        out = out.loc[[], :]
    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame)
def has_rownames(_data: DataFrame) -> bool:
    """Detect if a data frame has row names

    Aliases `has_index`

    Args:
        _data: The data frame to check

    Returns:
        True if the data frame has index otherwise False.

    """
    return not isinstance(_data.index, RangeIndex)


has_index = has_rownames  # pylint: disable=invalid-name


@register_verb(DataFrame)
def remove_rownames(_data: DataFrame) -> DataFrame:
    """Remove the index/rownames of a data frame

    Aliases `remove_index`, `drop_index`, `remove_rownames`

    Args:
        _data: The data frame

    Returns:
        The data frame with index removed

    """
    return _data.reset_index(drop=True)


remove_index = drop_index = remove_rownames  # pylint: disable=invalid-name


@register_verb(DataFrame, context=Context.SELECT)
def rownames_to_column(_data: DataFrame, var="rowname") -> DataFrame:
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

    return remove_rownames(mutate(_data, **{var: _data.index}, _before=1))


index_to_column = rownames_to_column  # pylint: disable=invalid-name


@register_verb(DataFrame, context=Context.SELECT)
def rowid_to_column(
    _data: DataFrame, var="rowid", base0_: bool = False
) -> DataFrame:
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

    base = int(not base0_)
    return remove_rownames(
        mutate(_data, **{var: range(base, _data.shape[0] + base)}, _before=1)
    )


@register_verb(DataFrame, context=Context.SELECT)
def column_to_rownames(_data: DataFrame, var: str = "rowname") -> DataFrame:
    """Set rownames/index with one column, and remove it

    Aliases `column_to_index`

    Args:
        _data: The data frame
        var: The column to conver to the rownames

    Returns:
        The data frame with the column converted to rownames
    """
    if has_rownames(_data):
        raise ValueError("`_data` must be a data frame without row names.")

    from ..dplyr.mutate import mutate

    try:
        rownames = [str(name) for name in _data[var]]
    except KeyError:
        raise ColumnNotExistingError(
            f"Column `{var}` does not exist."
        ) from None
    out = mutate(_data, **{var: None})
    out.index = rownames
    return out


column_to_index = column_to_rownames  # pylint: disable=invalid-name

# Helpers ------------------------------------------------------------------


def _pos_from_before_after_names(
    before: Union[str, int],
    after: Union[str, int],
    names: List[str],
    base0: bool,
) -> int:
    """Get the position to insert from before and after"""
    if before is not None:
        before, base0 = _check_names_before_after(before, names, base0)
    if after is not None:
        after, base0 = _check_names_before_after(after, names, base0)
    return _pos_from_before_after(before, after, len(names), base0)


def _check_names_before_after(
    pos: Union[str, int], names: List[str], base0: bool
) -> int:
    """Get position by given index or name"""
    if not isinstance(pos, str):
        return pos, base0
    try:
        return names.index(pos), True
    except ValueError:
        raise ColumnNotExistingError(
            f"Column `{pos}` does not exist."
        ) from None


def _cbind_at(
    data: DataFrame, df: DataFrame, pos: int, _name_repair: Union[str, Callable]
) -> DataFrame:
    """Column bind at certain pos, 0-based"""
    from ..dplyr import bind_cols

    part1 = data.iloc[:, :pos]
    part2 = data.iloc[:, pos:]
    return part1 >> bind_cols(df, part2, _name_repair=_name_repair)


def _pos_from_before_after(
    before: int, after: int, length: int, base0: bool
) -> int:
    """Get the position to insert from before and after"""
    if before is not None and after is not None:
        raise ValueError("Can't specify both `_before` and `_after`.")

    if before is None and after is None:
        return length

    if after is not None:
        return position_after(after, length, base0)
    return position_at(before, length, base0)


def _rbind_at(data: DataFrame, df: DataFrame, pos: int) -> DataFrame:
    """Row bind at certain pos, 0-based"""
    from ..dplyr import bind_rows

    part1 = data.iloc[:pos, :]
    part2 = data.iloc[pos:, :]
    return bind_rows(part1, df, part2)
