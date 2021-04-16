"""Core utilities"""

import logging
from functools import singledispatch
from copy import deepcopy
from typing import Any, Iterable, List, Mapping, Optional, Union

import numpy
from pandas import DataFrame
from pandas.core.flags import Flags
from pandas.core.indexes.base import Index
from pandas.core.series import Series
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy
from pandas.core.groupby.ops import BaseGrouper
from pipda.symbolic import Reference

from varname import argname

from .exceptions import (
    ColumnNameInvalidError, ColumnNotExistingError, NameNonUniqueError
)
from .types import DataFrameType, StringOrIter, is_scalar
from .names import repair_names
from .defaults import DEFAULT_COLUMN_PREFIX

# logger
logger = logging.getLogger('datar') # pylint: disable=invalid-name
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler() # pylint: disable=invalid-name
stream_handler.setFormatter(logging.Formatter(
    '[%(asctime)s][%(name)s][%(levelname)7s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(stream_handler)

def list_diff(list1: Iterable[Any], list2: Iterable[Any]) -> List[Any]:
    """Get the difference between two lists and keep the order

    Args:
        list1: The first list
        list2: The second list

    Returns:
        The list1 elements that don't exist in list2.
    """
    return [elem for elem in list1 if elem not in list2]

def check_column(column: Any) -> None:
    """Check if a column is valid

    Args:
        column: The column

    Raises:
        ColumnNameInvalidError: When the column name is invalid
    """
    from .middlewares import Inverted
    if not isinstance(column, (
            (int, str, list, set, tuple, Inverted, slice, Series, Index)
    )):
        raise ColumnNameInvalidError(
            'Invalid column, expected int, str, list, tuple, c(), '
            f'f.column, ~c() or ~f.column, got {type(column)}'
        )

def expand_collections(
        collections: Any,
        pool: Optional[Iterable[Any]] = None
) -> List[Any]:
    """Expand and flatten all iterables in the collections

    Args:
        collections: The collections of elements or iterables

    Returns:
        The flattened list
    """
    from .middlewares import Negated
    if isinstance(collections, Negated):
        return collections.evaluate(pool)
    if isinstance(collections, slice):
        return sanitize_slice(collections, pool, raise_nonexists=False)
    if is_scalar(collections) or isinstance(collections, Series):
        return [collections]
    ret = []
    for collection in collections:
        ret.extend(expand_collections(collection, pool))
    return ret

def sanitize_slice(
        slc: slice,
        all_columns: Optional[List[Union[str, int]]] = None,
        raise_nonexists: bool = True
) -> List[int]:
    """Sanitize slice objects, and compile it into a list of indexes

    Args:
        slc: The slice object
        all_columns: All columns used to convert names into indexes
            If it is not provided, the slice is expected to be all integers
        raise_nonexists: Raise error when a column doesnot exist?
            Requires `all_columns`.

    Returns:
        A list of indexes

    Raises:
        ColumnNotExistingError: When a column does not exist
    """
    start, stop, step = slc.start, slc.stop, slc.step
    if all_columns is None:
        # Treated as plain slice
        if isinstance(start, str) or isinstance(stop, str):
            raise ValueError(
                '`all_columns` is required when start/stop of slice is '
                'column name.'
            )
        step = 1 if step is None else step
        if start is None:
            start = 0
        if stop is None:
            stop = 0

        out = []
        out_append = out.append
        i = start
        while (i < stop) if step > 0 else (i > stop):
            out_append(i)
            i += step
        return out

    # all_columns defined
    if isinstance(start, str):
        if start not in all_columns:
            raise ColumnNotExistingError(f'Column `{start}` does not exist.')
        start = all_columns.index(start)
    if isinstance(stop, str):
        if stop not in all_columns:
            raise ColumnNotExistingError(f'Column `{stop}` does not exist.')
        stop = all_columns.index(stop) + 1

    all_len = len(all_columns)
    start = 0 if start is None else start
    if start < 0:
        start += all_len

    stop = len(all_columns) if stop is None else stop
    if stop < 0:
        stop += all_len + 1

    if step == 0:
        stop -= 1
        step = 1

    out = sanitize_slice(slice(start, stop, step))
    if raise_nonexists:
        for i in out:
            if i >= all_len:
                raise ColumnNotExistingError(
                    f'Column at location {i} does not exist.'
                )

    return out

def vars_select(
        all_columns: Iterable[str],
        *columns: Any,
        raise_nonexists: bool = True
) -> List[int]:
    """Select columns

    Args:
        all_columns: The column pool to select
        *columns: arguments to select from the pool
        raise_nonexist: Whether raise exception when column not exists
            in the pool
        return_indexes: Whether return indexes instead of column names
            This is useful when there are duplicated names

    Returns:
        The selected indexes for columns

    Raises:
        ColumnNameInvalidError: When the column is invalid to select
        ColumnNotExistingError: When the column does not exist in the pool
    """
    from .middlewares import Inverted
    all_columns = list(all_columns)

    selected = []
    selected_append = selected.append
    for column in columns:
        if column is None:
            continue

        check_column(column)
        if isinstance(column, int): # 1, -1
            # -1 will do select instead of removal
            if column not in selected:
                selected_append(column)
        elif isinstance(column, (list, tuple)): # ['x','y'] or [0,2]
            idxes = vars_select(all_columns, *column)
            for idx in idxes:
                if idx not in selected:
                    selected_append(idx)
        elif isinstance(column, Inverted):
            selected.extend(column.evaluate(all_columns, raise_nonexists))
        elif isinstance(column, slice):
            idxes = sanitize_slice(column, all_columns, raise_nonexists)
            for idx in idxes:
                if idx not in selected:
                    selected_append(idx)
        else:
            if isinstance(column, Series):
                column = column.name
            if column not in all_columns and raise_nonexists:
                raise ColumnNotExistingError(
                    f"Column `{column}` does not exist."
                )

            selected_append(all_columns.index(column))
    return selected

def series_expandable(
        df_or_series: Union[DataFrame, Series],
        series_or_df: Union[DataFrame, Series]
) -> bool:
    """Check if a series is expandable"""
    if (not isinstance(df_or_series, (Series, DataFrame)) or
            not isinstance(series_or_df, (Series, DataFrame))):
        return False

    if type(df_or_series) is type(series_or_df):
        if df_or_series.shape[0] < series_or_df.shape[0]:
            series, df = df_or_series, series_or_df
        else:
            df, series = df_or_series, series_or_df
    elif isinstance(df_or_series, Series):
        series, df = df_or_series, series_or_df
    else:
        df, series = df_or_series, series_or_df

    return series.index.name in df.columns

def series_expand(series: Union[DataFrame, Series], df: DataFrame):
    """Expand the series to the scale of a dataframe"""
    if isinstance(series, DataFrame):
        #assert series.shape[1] == 1
        series = series.iloc[:, 0]
    return df[series.index.name].map(series)

def align_value(
        value: Any,
        data: DataFrameType
) -> Any:
    """Normalize possible series data to add to the data or compare with
    other series of the data"""
    from ..base.constants import NA

    if is_scalar(value):
        return value

    if series_expandable(value, data):
        return series_expand(value, data)

    len_series = (
        value.shape[0] if isinstance(value, (DataFrame, Series))
        else len(value)
    )

    if len_series == data.shape[0]:
        return value
    if len_series == 0:
        return NA

    if data.shape[0] % len_series == 0:
        nrepeat = data.shape[0] // len_series
        if isinstance(value, (list, tuple)):
            return value * nrepeat
        # numpy.ndarray
        return value.repeat(nrepeat)
    return value

def copy_flags(df1: DataFrame, flags: Union[DataFrameType, Flags]) -> None:
    """Deep copy the flags from one dataframe to another"""
    if isinstance(flags, DataFrame):
        flags = flags.flags
    elif isinstance(flags, DataFrameGroupBy):
        flags = flags.obj.flags

    for key in dir(flags):
        if key.startswith('_'):
            continue

        setattr(df1.flags, key, deepcopy(getattr(flags, key)))

def df_assign_item(
        df: DataFrame,
        item: str,
        value: Any,
        allow_dups: bool = False,
        allow_incr: bool = True
) -> None:
    """Assign an item to a dataframe"""
    value = align_value(value, df)
    try:
        value = value.values
    except AttributeError:
        ...

    lenval = 1 if is_scalar(value) else len(value)

    if allow_incr and df.shape[0] == 1 and lenval > 1:
        if df.shape[1] == 0: # 0-column df
            # Otherwise, cannot set a frame with no defined columns
            df['__assign_placeholder__'] = 1
        # add rows inplace
        for i in range(lenval - 1):
            df.loc[i+1] = df.iloc[0, :]

        if '__assign_placeholder__' in df:
            df.drop(columns=['__assign_placeholder__'], inplace=True)

    if not allow_dups:
        df[item] = value
    else:
        df.insert(df.shape[1], item, value, allow_duplicates=True)

def objectize(data: Any) -> Any:
    """Get the object instead of the GroupBy object"""
    if isinstance(data, (SeriesGroupBy, DataFrameGroupBy)):
        return data.obj
    return data

def categorize(data: Any) -> Any:
    """Get the Categorical object"""
    try:
        return data.cat
    except AttributeError:
        return data

@singledispatch
def to_df(data: Any, name: Optional[str] = None) -> DataFrame:
    """Convert an object to a data frame"""
    if is_scalar(data):
        data = [data]

    if name is None:
        return DataFrame(data)

    return DataFrame({name: data})

@to_df.register(numpy.ndarray)
def _(data: numpy.ndarray, name: Optional[str] = None) -> DataFrame:
    if len(data.shape) == 1:
        return DataFrame(data, columns=[name]) if name else DataFrame(data)

    ncols = data.shape[1]
    if isinstance(name, Iterable) and len(name) == ncols:
        return DataFrame(data, columns=name)
    if len(name) == 1 and name and isinstance(name, str):
        return DataFrame(data, columns=[name])
    # ignore the name
    return DataFrame(data)

@to_df.register(DataFrame)
def _(data: DataFrame, name: Optional[str] = None) -> DataFrame:
    if name is None:
        return data
    return DataFrame({f"{name}${col}": data[col] for col in data.columns})

@to_df.register(Series)
def _(data: Series, name: Optional[str] = None) -> DataFrame:
    name = name or data.name
    return data.to_frame(name=name)

@to_df.register(SeriesGroupBy)
def _(data: SeriesGroupBy, name: Optional[str] = None) -> DataFrame:
    name = name or data.obj.name
    return data.obj.to_frame(name=name).groupby(data.grouper, dropna=False)

def group_df(
        df: DataFrame,
        grouper: Union[BaseGrouper, StringOrIter],
        drop: Optional[bool] = None
) -> DataFrameGroupBy:
    """Group a dataframe"""
    from ..dplyr import group_by_drop_default
    if drop is None:
        drop = group_by_drop_default(df)

    return df.groupby(
        grouper,
        as_index=False,
        sort=True,
        dropna=False,
        group_keys=False,
        observed=drop
    )

def check_column_uniqueness(df: DataFrame, msg: Optional[str] = None) -> None:
    """Check if column names are unique of a dataframe"""
    try:
        repair_names(df.columns.tolist(), repair="check_unique")
    except NameNonUniqueError as error:
        raise ValueError(msg or str(error)) from None

def dict_insert_at(
        container: Mapping[str, Any],
        poskeys: Iterable[str],
        value: Mapping[str, Any],
        remove: bool = False
) -> Mapping[str, Any]:
    """Insert value to a certain position of a dict"""
    ret_items = []
    ret_items_append = ret_items.append
    matched = False
    for key, val in container.items():
        if key == poskeys[0]:
            matched = True
            ret_items.extend(value.items())
            if not remove:
                ret_items_append((key, val))
        elif matched and key in poskeys:
            if not remove:
                ret_items_append((key, val))
        elif matched and key not in poskeys:
            matched = False
            ret_items_append((key, val))
        else:
            ret_items_append((key, val))

    return dict(ret_items)

def name_mutatable_args(
        *args: Union[Series, DataFrame, Mapping[str, Any]],
        **kwargs: Any
) -> Mapping[str, Any]:
    """Convert all mutatable arguments to named mappings, which can be easier
    to mutate later on.

    If there are Expression objects, keep it. So if an objects have multiple
    names and it's built by an Expression, then the name might get lost here.

    Examples:

        >>> s = Series([1], name='a')
        >>> name_mutatable_args(s, b=2)
        >>> # {'a': s, b: 2}
        >>> df = DataFrame({'x': [3], 'y': [4]})
        >>> name_mutatable_args(df)
        >>> # {'x': Series([3]), 'y': Series([4])}
        >>> name_mutatable_args(d=df)
        >>> # {'d$x': Series([3]), 'd$y': Series([4])}
    """
    ret = {} # order kept

    for i, arg in enumerate(args):
        if isinstance(arg, Series):
            ret[arg.name] = arg
        elif isinstance(arg, dict):
            ret.update(arg)
        elif isinstance(arg, DataFrame):
            ret.update(arg.to_dict('series'))
        elif isinstance(arg, Reference):
            ret[arg.ref] = arg
        else:
            ret[f"{DEFAULT_COLUMN_PREFIX}{i}"] = arg

    for key, val in kwargs.items():
        if isinstance(val, DataFrame):
            val = val.to_dict('series')

        if isinstance(val, dict):
            existing_keys = [
                ret_key for ret_key in ret
                if ret_key == key or ret_key.startswith(f"{key}$")
            ]
            if existing_keys:
                ret = dict_insert_at(ret, existing_keys, val, remove=True)
            else:
                for dkey, dval in val.items():
                    ret[f"{key}${dkey}"] = dval
        else:
            ret[key] = val
    return ret

def arg_match(arg: Any, values: Iterable[Any], errmsg=Optional[str]) -> Any:
    """Make sure arg is in one of the values.

    Mimics `rlang::arg_match`.
    """
    if not errmsg:
        values = list(values)
        name = argname(arg, pos_only=True)
        errmsg = f'`{name}` must be one of {values}.'
    if arg not in values:
        raise ValueError(errmsg)
    return arg

def copy_attrs(
        df1: DataFrame,
        df2: DataFrame,
        deep: bool = False,
        group: bool = False
) -> None:
    """Copy attrs from df2 to df1"""
    for key, val in df2.attrs.items():
        if group or not key.startswith('group_'):
            df1.attrs[key] = deepcopy(val) if deep else val
