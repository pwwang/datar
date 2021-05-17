"""Core utilities"""

import logging
import inspect
from functools import singledispatch
from copy import deepcopy
from typing import Any, Callable, Iterable, List, Mapping, Optional, Union

import numpy
from pandas import DataFrame, Series
from pandas.core.dtypes.common import is_categorical_dtype
from pipda.symbolic import Reference

from varname import argname

from .exceptions import ColumnNotExistingError, NameNonUniqueError
from .types import is_scalar
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

def vars_select(
        all_columns: Iterable[str],
        *columns: Any,
        raise_nonexists: bool = True,
        base0: Optional[bool] = None
) -> List[int]:
    """Select columns

    Args:
        all_columns: The column pool to select
        *columns: arguments to select from the pool
        raise_nonexist: Whether raise exception when column not exists
            in the pool
        base0: Whether indexes are 0-based if columns are selected by indexes.
            If not given, will use `datar.base.getOption('index.base.0')`

    Returns:
        The selected indexes for columns

    Raises:
        ColumnNotExistingError: When the column does not exist in the pool
            and raise_nonexists is True.
    """
    from .collections import Collection
    from ..base import unique
    columns = (
        column.name if isinstance(column, Series) else column
        for column in columns
    )
    selected = Collection(*columns, pool=list(all_columns), base0=base0)
    if raise_nonexists and selected.unmatched and selected.unmatched != {None}:
        raise ColumnNotExistingError(
            f"Columns `{selected.unmatched}` do not exist."
        )
    return unique(selected).astype(int)

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
        data: DataFrame
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

def categorize(data: Any) -> Any:
    """Get the Categorical object"""
    if not is_categorical_dtype(data):
        return data
    if isinstance(data, Series):
        return data.values
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

# @to_df.register(SeriesGroupBy)
# def _(data: SeriesGroupBy, name: Optional[str] = None) -> DataFrame:
#     name = name or data.obj.name
#     return data.obj.to_frame(name=name).groupby(data.grouper, dropna=False)

def check_column_uniqueness(df: DataFrame, msg: Optional[str] = None) -> None:
    """Check if column names are unique of a dataframe"""
    uniq = set()
    for col in df.columns:
        if col not in uniq:
            uniq.add(col)
        else:
            msg = msg or 'Name is not unique'
            raise NameNonUniqueError(f"{msg}: {col}")

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

def nargs(fun: Callable) -> int:
    """Get the number of arguments of a function"""
    return len(inspect.signature(fun).parameters)

def position_at(pos: int, length: int, base0: Optional[bool] = None) -> int:
    """Get the 0-based position right at the given pos

    When `pos` is negative, it acts like 0-based, meaning `-1` will anyway
    represent the last position regardless of `base0`

    Args:
        pos: The given position
        length: The length of the pool

    Returns:
        The 0-based position
    """
    from .collections import Collection
    return Collection(pos, pool=length, base0=base0)[0]
    # base0 = get_option('index.base.0', base0)
    # if not base0 and pos == 0:
    #     raise IndexError('Index 0 given for 1-based indexing.')
    # return pos - int(not base0) if pos >= 0 else pos + length

def position_after(pos: int, length: int, base0: Optional[bool] = None) -> int:
    """Get the 0-based position right at the given pos

    Args:
        pos: The given position
        length: The length of the pool

    Returns:
        The position before the given position
    """
    base0 = get_option('index.base.0', base0)
    # after 0 with 1-based, should insert to first column
    if not base0 and pos == 0:
        return 0

    return position_at(pos, length, base0) + 1

def get_option(key: str, value: Any = None) -> Any:
    """Get the option with key.

    This is for interal use mostly.

    This is a shortcut for:
    >>> if value is not None:
    >>>     return value
    >>> from datar.base import getOption
    >>> return getOption(key)
    """
    if value is not None:
        return value
    from ..base import getOption
    return getOption(key)
