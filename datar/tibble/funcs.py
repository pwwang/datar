"""Functions ported from tidyverse-tibble"""
import itertools
from typing import Any, Callable, Iterable, List, Mapping, Union, Optional

from pandas import DataFrame, Series, RangeIndex
from pipda import Context, register_func, register_verb
from pipda.utils import Expression
from pipda.symbolic import DirectRefAttr, DirectRefItem
from varname import argname, varname
from varname.utils import VarnameRetrievingError

from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.utils import (
    copy_attrs, df_assign_item, get_option, position_after,
    position_at, to_df, logger
)
from ..core.names import repair_names
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.types import is_scalar
from ..core.exceptions import ColumnNotExistingError
from ..base import setdiff

def tibble(
        *args: Any,
        _name_repair: Union[str, Callable] = 'check_unique',
        _rows: Optional[int] = None,
        **kwargs: Any
) -> DataFrame:
    # pylint: disable=too-many-statements,too-many-branches
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

    Returns:
        A constructed dataframe
    """
    if not args and not kwargs:
        df = DataFrame() if not _rows else DataFrame(index=range(_rows))
        try:
            df.__dfname__ = varname(raise_exc=False)
        except VarnameRetrievingError: # pragma: no cover
            df.__dfname__ = None
        return df

    try:
        argnames = argname(args, vars_only=False, pos_only=True)
        if len(argnames) != len(args):
            raise VarnameRetrievingError
    except VarnameRetrievingError:
        argnames = [f"{DEFAULT_COLUMN_PREFIX}{i}" for i in range(len(args))]

    name_values = zip(argnames, args)
    name_values = itertools.chain(name_values, kwargs.items())
    # cannot do it with Mappings, same keys will be lost
    names = []
    values = []
    for name, value in name_values:
        names.append(name)
        values.append(value)

    names = repair_names(names, repair=_name_repair)
    df = None

    for name, arg in zip(names, values):
        if arg is None:
            continue
        if isinstance(arg, Expression):
            arg = arg(df, Context.EVAL.value)

        if isinstance(arg, dict):
            arg = tibble(**arg)

        elif isinstance(arg, Series) and name in argnames:
            name = arg.name

        if df is None:
            if isinstance(arg, DataFrame):
                # df = arg.copy()
                # DataFrameGroupBy.copy copied into DataFrameGroupBy
                df = DataFrame(arg).copy()
                if name not in argnames:
                    df.columns = [f'{name}${col}' for col in df.columns]

            else:
                df = to_df(arg, name)
        elif isinstance(arg, DataFrame):
            for col in arg.columns:
                df_assign_item(
                    df,
                    f'{name}${col}' if name not in argnames else col,
                    arg[col],
                    allow_dups=True
                )
        else:
            df_assign_item(df, name, arg, allow_dups=True)

    if df is None:
        df = DataFrame()
    try:
        df.__dfname__ = varname(raise_exc=False)
    except VarnameRetrievingError: # pragma: no cover
        df.__dfname__ = None # still raises in some cases

    if not kwargs and len(args) == 1 and isinstance(args[0], DataFrame):
        copy_attrs(df, args[0])
    return df

def tibble_row(
        *args: Any,
        _name_repair: Union[str, Callable] = 'check_unique',
        **kwargs: Any
) -> DataFrame:
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
    if not args and not kwargs:
        df = DataFrame(index=[0]) # still one row
    else:
        df = tibble(*args, **kwargs, _name_repair=_name_repair)

    if df.shape[0] > 1:
        raise ValueError("All arguments must be size one, use `[]` to wrap.")
    try:
        df.__dfname__ = varname(raise_exc=False)
    except VarnameRetrievingError: # pragma: no cover
        df.__dfname__ = None
    return df

@register_func(None, context=Context.EVAL)
def fibble(
        *args: Any,
        _name_repair: Union[str, Callable] = 'check_unique',
        _rows: Optional[int] = None,
        **kwargs: Any
) -> DataFrame:
    """A function of tibble that can be used as an argument of verbs

    Since `tibble` can recycle previous items, for example:
        >>> df >> tibble(x=1, y=f.x+1)
        >>> # x y
        >>> # 1 2

    It gets confused when it is used as an argument of a verb, the we can't tell
    whether `f` if a proxy for the data of the verb or the data frame that
    `tibble` is constructing. So then here is the function to be used as a verb
    argument so `f` refers to the data of the verb. Note that in such a case,
    the items coming in previously cannot be recycled.

    See `tibble` for details.

    """
    return tibble(*args, **kwargs, _name_repair=_name_repair, _rows=_rows)

def tribble(*dummies: Any) -> DataFrame:
    """Create dataframe using an easier to read row-by-row layout

    Unlike original API that uses formula (`f.col`) to indicate the column
    names, we use `f.col` to indicate them.

    Args:
        *dummies: Arguments specifying the structure of a dataframe
            Variable names should be specified with `f.name`

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
    columns = []
    data = []
    for dummy in dummies:
        # columns
        if isinstance(dummy, (DirectRefAttr, DirectRefItem)):
            columns.append(dummy.ref)
        elif not columns:
            raise ValueError(
                'Must specify at least one column using the `f.<name>` syntax.'
            )
        else:
            if not data:
                data.append([])
            if len(data[-1]) < len(columns):
                data[-1].append(dummy)
            else:
                data.append([dummy])

    ret = (
        DataFrame(data, columns=columns) if data
        else DataFrame(columns=columns)
    )
    try:
        ret.__dfname__ = varname(raise_exc=False)
    except VarnameRetrievingError: # pragma: no cover
        ret.__dfname__ = None
    return ret

def enframe(
        x: Optional[Union[Iterable, Mapping]],
        name: Optional[str] = "name",
        value: str = "value",
        _base0: Optional[bool] = None
) -> DataFrame:
    """Converts mappings or lists to one- or two-column data frames.

    Args:
        x: a list, a dictionary or a dataframe with one or two columns
        name: and
        value: value Names of the columns that store the names and values.
            If `None`, a one-column dataframe is returned.
            `value` cannot be `None`
        _base0: Whether the indexes for lists converted to name are 0-based
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

    if len(getattr(x, 'shape', ())) > 1:
        raise ValueError(
            f"`x` must not have more than one dimension, got {len(x.shape)}."
        )

    if not name and isinstance(x, dict):
        x = x.values()

    elif name:
        if not isinstance(x, dict):
            _base0 = get_option('index.base.0', _base0)
            names = (i + int(not _base0) for i in range(len(x)))
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
        extra_contexts={'_before': Context.SELECT, '_after': Context.SELECT}
)
def add_row(
        _data: DataFrame,
        *args: Any,
        _before: Optional[int] = None,
        _after: Optional[int] = None,
        _base0: Optional[bool] = None,
        **kwargs: Any
) -> DataFrame:
    """Add one or more rows of data to an existing data frame.

    Aliases: `add_case`

    Args:
        _data: Data frame to append to.
        *args: and
        **kwargs: Name-value pairs to add to the data frame.
        _before: and
        _after: row index where to add the new rows.
            (default to add after the last row)
        _base0: Whether `_before` and `_after` are 0-based or not.

    Returns:
        The dataframe with the added rows

    """
    if (
            isinstance(_data, DataFrameGroupBy) and
            not isinstance(_data, DataFrameRowwise)
    ):
        raise ValueError("Can't add rows to grouped data frames.")

    from ..dplyr.group_by import group_by_drop_default
    from ..dplyr.group_data import group_vars

    if not args and not kwargs:
        df = DataFrame(index=[0], columns=_data.columns)
    else:
        df = tibble(*args, **kwargs)
        if df.shape[0] == 0:
            for col in _data.columns:
                df[col] = Series(dtype=_data[col].dtype)

    extra_vars = setdiff(df.columns, _data.columns)
    if extra_vars:
        raise ValueError(f"New rows can't add columns: {extra_vars}")

    pos = _pos_from_before_after(_before, _after, _data.shape[0], _base0)
    out = _rbind_at(_data, df, pos)

    if isinstance(_data, DataFrameRowwise):
        out = DataFrameRowwise(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )

    copy_attrs(out, _data)
    return out

add_case = add_row # pylint: disable=invalid-name

@register_verb(
        DataFrame,
        context=Context.EVAL,
        extra_contexts={'_before': Context.SELECT, '_after': Context.SELECT}
)
def add_column(
        _data: DataFrame,
        *args: Any,
        _before: Optional[Union[str, int]] = None,
        _after: Optional[Union[str, int]] = None,
        _name_repair: Union[str, Callable] = 'check_unique',
        _base0: Optional[bool] = None,
        **kwargs: Any
) -> DataFrame:
    """Add one or more columns to an existing data frame.

    Args:
        _data: Data frame to append to
        *args: and
        **kwargs: Name-value pairs to add to the data frame
        _before: and
        _after: Column index or name where to add the new columns
            (default to add after the last column)
        _base0: Whether `_before` and `_after` are 0-based if they are index.
            if not given, will be determined by `getOption('index_base_0')`,
            which is `False` by default.

    Returns:
        The dataframe with the added columns
    """
    from ..dplyr.group_by import group_by_drop_default
    from ..dplyr.group_data import group_vars

    df = tibble(*args, **kwargs, _name_repair='minimal')

    if df.shape[1] == 0:
        return _data.copy()

    if df.shape[0] != _data.shape[0]:
        if df.shape[0] != 1:
            raise ValueError(
                f"New columns have {df.shape[0]} rows, "
                f"but `_data` has {_data.shape[0]}."
            )
        df = df.iloc[[0] * _data.shape[0], :].reset_index(drop=True)

    pos = _pos_from_before_after_names(
        _before,
        _after,
        _data.columns.tolist(),
        _base0
    )
    out = _cbind_at(_data, df, pos, _name_repair)

    if isinstance(_data, DataFrameGroupBy):
        out = _data.__class__(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )

    copy_attrs(out, _data)
    return out

@register_verb(DataFrame)
def has_rownames(_data: DataFrame) -> bool:
    """Detect if a data frame has row names

    Aliases: `has_index`

    Args:
        _data: The data frame to check

    Returns:
        True if the data frame has index otherwise False.

    """
    return not isinstance(_data.index, RangeIndex)

has_index = has_rownames # pylint: disable=invalid-name

@register_verb(DataFrame)
def remove_rownames(_data: DataFrame) -> DataFrame:
    """Remove the index/rownames of a data frame

    Aliases: `remove_index`, `drop_index`, `remove_rownames`

    Args:
        _data: The data frame

    Returns:
        The data frame with index removed

    """
    return _data.reset_index(drop=True)

remove_index = drop_index = remove_rownames # pylint: disable=invalid-name

@register_verb(DataFrame, context=Context.SELECT)
def rownames_to_column(_data: DataFrame, var="rowname") -> DataFrame:
    """Add rownames as a column

    Aliases: `index_to_column`

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

index_to_column = rownames_to_column # pylint: disable=invalid-name

@register_verb(DataFrame, context=Context.SELECT)
def rowid_to_column(
        _data: DataFrame,
        var="rowid",
        _base0: bool = False
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
    base = int(not _base0)
    return remove_rownames(mutate(
        _data,
        **{var: range(base, _data.shape[0] + base)},
        _before=1
    ))

@register_verb(DataFrame, context=Context.SELECT)
def column_to_rownames(_data: DataFrame, var: str = "rowname") -> DataFrame:
    """Set rownames/index with one column, and remove it

    Aliases: `column_to_index`

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

column_to_index = column_to_rownames # pylint: disable=invalid-name

# Helpers ------------------------------------------------------------------

def _pos_from_before_after_names(
        before: Optional[Union[str, int]],
        after: Optional[Union[str, int]],
        names: List[str],
        base0: Optional[bool]
) -> int:
    """Get the position to insert from before and after"""
    if before is not None:
        before, base0 = _check_names_before_after(before, names, base0)
    if after is not None:
        after, base0 = _check_names_before_after(after, names, base0)
    return _pos_from_before_after(before, after, len(names), base0)

def _check_names_before_after(
        pos: Union[str, int],
        names: List[str],
        base0: Optional[bool]
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
        data: DataFrame,
        df: DataFrame,
        pos: int,
        _name_repair: Union[str, Callable]
) -> DataFrame:
    """Column bind at certain pos, 0-based"""
    from ..dplyr import bind_cols
    part1 = data.iloc[:, :pos]
    part2 = data.iloc[:, pos:]
    return bind_cols(part1, df, part2, _name_repair=_name_repair)

def _pos_from_before_after(
        before: Optional[int],
        after: Optional[int],
        length: int,
        base0: bool
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
