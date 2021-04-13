"""Core utilities"""

import logging
from functools import singledispatch
from copy import deepcopy
from typing import Any, Callable, Iterable, List, Mapping, Optional, Union

import numpy
from pandas import DataFrame, Categorical
from pandas.core.flags import Flags
from pandas.core.series import Series
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy
from pandas.core.groupby.ops import BaseGrouper
from pandas.core.dtypes.common import is_categorical_dtype
from pipda.symbolic import DirectRefAttr, DirectRefItem

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

def list_intersect(list1: Iterable[Any], list2: Iterable[Any]) -> List[Any]:
    """Get the intersect between two lists and keep the order

    Args:
        list1: The first list
        list2: The second list

    Returns:
        The list1 elements that also exist in list2.
    """
    return [elem for elem in list1 if elem in list2]

def list_union(list1: Iterable[Any], list2: Iterable[Any]) -> List[Any]:
    """Get the union between two lists and keep the order

    Args:
        list1: The first list
        list2: The second list

    Returns:
        The elements with elements in either list1 or list2
    """
    return list1 + list_diff(list1=list2, list2=list1)

def check_column(column: Any) -> None:
    """Check if a column is valid

    Args:
        column: The column

    Raises:
        ColumnNameInvalidError: When the column name is invalid
    """
    from .middlewares import Inverted
    if not isinstance(column, (
            (int, str, list, set, tuple, Inverted, slice)
    )):
        raise ColumnNameInvalidError(
            'Invalid column, expected int, str, list, tuple, c(), '
            f'f.column, ~c() or ~f.column, got {type(column)}'
        )

def expand_collections(collections: Any) -> List[Any]:
    """Expand and flatten all iterables in the collections

    Args:
        collections: The collections of elements or iterables

    Returns:
        The flattened list
    """
    if is_scalar(collections):
        return [collections]
    ret = []
    for collection in collections:
        ret.extend(expand_collections(collection))
    return ret

def filter_columns(
        all_columns: Iterable[str],
        match: Union[Iterable[str], str],
        ignore_case: bool,
        func: Callable[[str, str], bool]
) -> List[str]:
    """Filter the columns with given critera

    Args:
        all_columns: The column pool to filter
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        func: A function to define how to filter.

    Returns:
        A list of matched vars
    """
    if not isinstance(match, (tuple, list, set)):
        match = [match]

    ret = []
    for mat in match:
        for column in all_columns:
            if column in ret:
                continue
            if (func(
                    mat.lower() if ignore_case else mat,
                    column.lower() if ignore_case else column
            )):
                ret.append(column)
    return ret

def sanitize_slice(slc: slice, all_columns: List[str]) -> slice:
    """Sanitize slice objects"""
    int_start, int_stop, step = slc.start, slc.stop, slc.step
    if isinstance(int_start, str):
        int_start = all_columns.index(int_start)
    if isinstance(int_stop, str):
        int_stop = all_columns.index(int_stop)

    int_stop += 1
    if step == 0:
        step = None
        int_stop -= 1
    return slice(int_start, int_stop, step)

def _expand_slice_dummy(
        elems: Union[slice, list, int, tuple, "Negated", "Inverted"],
        total: int,
        from_negated: bool = False
) -> List[int]:
    """Expand a dummy slice object"""
    from .middlewares import Negated, Inverted
    all_indexes = list(range(total))
    if isinstance(elems, int):
        return [elems + 1 if from_negated else elems]
    if isinstance(elems, slice):
        if from_negated:
            # we want [0, 1, 2, 3]
            # to be negated as [-1, -2, -3, -4]
            return [elem+1 for elem in all_indexes[elems]]
        return all_indexes[elems]
    if isinstance(elems, (list, tuple)):
        selected_indexes = sum(
            (_expand_slice_dummy(elem, total, from_negated) for elem in elems),
            []
        )
        return list_intersect(selected_indexes, all_indexes)
    if isinstance(elems, Negated):
        if from_negated:
            raise ValueError('Cannot nest negated selections.')
        selected_indexes = sum(
            (_expand_slice_dummy(elem, total, True) for elem in elems.elems),
            []
        )
        return [-elem for elem in selected_indexes]
    if isinstance(elems, Inverted):
        selected_indexes = sum(
            (_expand_slice_dummy(elem, total, from_negated)
             for elem in elems.elems),
            []
        )
        return list_diff(all_indexes, selected_indexes)
    raise TypeError(f'Unsupported type for slice expansion: {type(elems)!r}.')

def expand_slice(
        elems: Union[slice, list, "Negated", "Inverted"],
        total: Union[int, Iterable[int]]
) -> Union[List[int], List[List[int]]]:
    """Expand the slide in an iterable, in a groupby-aware way"""
    return _expand_slice_dummy(elems, total)

def vars_select(
        all_columns: Iterable[str],
        *columns: Any,
        raise_nonexist: bool = True
) -> List[str]:
    """Select columns

    Args:
        all_columns: The column pool to select
        *columns: arguments to select from the pool
        raise_nonexist: Whether raise exception when column not exists
            in the pool

    Returns:
        The selected columns

    Raises:
        ColumnNameInvalidError: When the column is invalid to select
        ColumnNotExistingError: When the column does not exist in the pool
    """
    from .middlewares import Inverted
    if not isinstance(all_columns, list):
        all_columns = list(all_columns)

    negs = [isinstance(column, Inverted) for column in columns]
    has_negs = any(negs)
    if has_negs and not all(negs):
        raise ColumnNameInvalidError(
            'Either none or all of the columns are negative.'
        )

    selected = []
    for column in columns:
        check_column(column)
        if isinstance(column, int): # 1, -1
            # -1 will do select instead of removal
            selected.append(all_columns[column])
        elif isinstance(column, (list, tuple, set)): # ['x', 'y']
            selected.extend(column)
        elif isinstance(column, Inverted):
            selected.extend(column.elems)
        elif isinstance(column, slice):
            selected.extend(all_columns[sanitize_slice(column, all_columns)])
        else:
            selected.append(column)

    if raise_nonexist:
        for sel in selected:
            if sel not in all_columns:
                raise ColumnNotExistingError(
                    f"Column `{sel}` doesn't exist."
                )

    if has_negs:
        selected = list_diff(all_columns, selected)
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

    data = objectize(data)
    value = objectize(value)

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

def update_df(df: DataFrame, df2: DataFrame) -> None:
    """Update the dataframe"""
    # DataFrame.update ignores nonexisting columns
    # and not keeps categories

    for col in df2.columns:
        df[col] = df2[col]

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
        allow_dups: bool = False
) -> None:
    """Assign an item to a dataframe"""
    value = align_value(value, df)
    try:
        value = value.values
    except AttributeError:
        ...

    lenval = 1 if is_scalar(value) else len(value)

    if df.shape[0] == 1 and lenval > 1:
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

def get_n_from_prop(
        total: int,
        n: Optional[int] = None,
        prop: Optional[float] = None
) -> int:
    """Get n from a proportion"""
    if n is None and prop is None:
        return 1
    if prop is not None:
        return int(float(total) * min(prop, 1.0))
    return min(n, total)

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

def groupby_apply(
        df: DataFrameGroupBy,
        func: Callable,
        groupdata: bool = False
) -> DataFrame:
    """Apply a function to DataFrameGroupBy object"""
    if groupdata:
        # df.groupby(group_keys=True).apply does not always add group as index
        g_keys = df.grouper.names
        def apply_func(subdf):
            if subdf is None or subdf.shape[0] == 0:
                return None
            ret = func(subdf)
            for key in g_keys:
                if key not in ret:
                    df_assign_item(ret, key, subdf[key].values[0])
                    if is_categorical_dtype(subdf[key]):
                        ret[key] = Categorical(
                            ret[key],
                            categories=subdf[key].cat.categories
                        )
            columns = list_union(g_keys, ret.columns)
            # keep the original order
            commcols = [col for col in df.obj.columns if col in columns]
            # make sure columns are included
            columns = list_union(commcols, list_diff(columns, commcols))
            return ret[columns]

        ret = df.apply(apply_func).reset_index(drop=True)
    else:
        ret = df.apply(func).reset_index(drop=True)

    copy_flags(ret, df)
    return ret

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
        elif isinstance(arg, (DirectRefAttr, DirectRefItem)):
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
