import sys
import inspect
from functools import singledispatch, wraps
from typing import Any, Callable, Iterable, List, Optional, Union

import numpy
from pandas import DataFrame
from pandas.core.series import Series
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy
import pipda
from pipda.context import Context, ContextBase
from pipda.function import register_func
from pipda.symbolic import DirectRefAttr
from pipda.utils import evaluate_args, evaluate_expr, evaluate_kwargs

from .exceptions import ColumnNameInvalidError, ColumnNotExistingError

IterableLiterals = (list, tuple, set, Iterable)
NumericType = Union[int, float]
NA = numpy.nan

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
    return list1 + list_diff(list2, list1)

def check_column(column: Any) -> None:
    """Check if a column is valid

    Args:
        column: The column

    Raises:
        ColumnNameInvalidError: When the column name is invalid
    """
    from .middlewares import Inverted, Across
    if not isinstance(column, (
            (int, str, list, set, tuple, Inverted, Across, slice)
    )):
        raise ColumnNameInvalidError(
            'Invalid column, expected int, str, list, tuple, c(), across(), '
            f'f.column, ~c() or ~f.column, got {type(column)}'
        )

def expand_collections(collections: Any) -> List[Any]:
    """Expand and flatten all iterables in the collections

    Args:
        collections: The collections of elements or iterables

    Returns:
        The flattened list
    """
    if (
            isinstance(collections, str) or
            not isinstance(collections, IterableLiterals)
    ):
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
    from .middlewares import Negated, Inverted, Collection
    if isinstance(total, int):
        return _expand_slice_dummy(elems, total)
    # return _expand_slice_grouped(elems, total)

def select_columns(
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
    from .middlewares import Inverted, Across
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
        elif isinstance(column, Across):
            selected.extend(column.evaluate(Context.SELECT))
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
    if isinstance(series, DataFrame):
        #assert series.shape[1] == 1
        series = series.iloc[:, 0]
    return df[series.index.name].map(series)

def align_value(
        value: Any,
        data: Union[DataFrame, DataFrameGroupBy]
) -> Any:
    """Normalize possible series data to add to the data or compare with
    other series of the data"""
    if isinstance(value, str) or not isinstance(value, IterableLiterals):
        return value

    if isinstance(data, DataFrameGroupBy):
        data = data.obj
    if isinstance(value, (DataFrameGroupBy, SeriesGroupBy)):
        value = value.obj

    if series_expandable(value, data):
        return series_expand(value, data)

    len_series = (
        value.shape[0] if isinstance(value, (DataFrame, Series))
        else len(value)
    )

    if len_series == data.shape[0]:
        return value
    if data.shape[0] % len_series == 0:
        nrepeat = data.shape[0] // len_series
        if isinstance(value, (list, tuple)):
            return value * nrepeat
        return value.append([value] * (nrepeat - 1))
    return value

def copy_df(
        df: Union[DataFrame, DataFrameGroupBy]
) -> Union[DataFrame, DataFrameGroupBy]:
    if isinstance(df, DataFrame):
        return df.copy()
    obj = df.obj.copy()
    return obj.groupby(df.grouper, dropna=False)

def df_assign_item(
        df: Union[DataFrame, DataFrameGroupBy],
        item: str,
        value: Any
) -> None:
    if isinstance(df, DataFrameGroupBy):
        df = df.obj
    try:
        value = value.values
    except AttributeError:
        ...

    df[item] = value

def objectize(data: Any) -> Any:
    if isinstance(data, (SeriesGroupBy, DataFrameGroupBy)):
        return data.obj
    return data

def categorize(data: Any) -> Any:
    try:
        return data.cat
    except AttributeError:
        return data

@singledispatch
def to_df(data: Any, name: Optional[str] = None) -> DataFrame:
    try:
        df = DataFrame(data, columns=[name]) if name else DataFrame(data)
    except ValueError:
        df = DataFrame([data], columns=[name]) if name else DataFrame([data])

    return df

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
    return data

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
    if n is None and prop is None:
        return 1
    if prop is not None:
        return int(float(total) * min(prop, 1.0))
    return min(n, total)


def _register_grouped_col0(
        func: Callable,
        context: ContextBase
) -> Callable:
    """Register a function with argument of no column as groupby aware"""

    @register_func(DataFrame, context=None)
    @wraps(func)
    def wrapper(
            _data: DataFrame,
            *args: Any,
            **kwargs: Any
    ) -> Any:
        _column = DirectRefAttr(_data, _data.columns[0])
        series = evaluate_expr(_column, _data, context)
        args = evaluate_args(args, _data, context.args)
        kwargs = evaluate_kwargs(kwargs, _data, context.kwargs)
        return func(series, *args, **kwargs)

    @wrapper.register(DataFrameGroupBy)
    def _(
            _data: DataFrameGroupBy,
            *args: Any,
            **kwargs: Any
    ) -> Any:
        _column = DirectRefAttr(_data, _data.obj.columns[0])
        series = evaluate_expr(_column, _data, context)
        args = evaluate_args(args, _data, context.args)
        kwargs = evaluate_kwargs(kwargs, _data, context.kwargs)
        return series.apply(func, *args, **kwargs)

    return wrapper

def _register_grouped_col1(
        func: Callable,
        context: ContextBase
) -> Callable:
    """Register a function with argument of single column as groupby aware"""

    @register_func(DataFrame, context=None)
    @wraps(func)
    def wrapper(
            # in case this is called directly (not in a piping env)
            # we should not have the _data argument
            # _data: DataFrame,
            # _column: Any,
            *args: Any,
            **kwargs: Any
    ) -> Any:
        # Let's if the function is called in a piping env
        # If so, the previous frame should be in functools
        # Otherwise, it should be pipda.function, where the wrapped
        # function should be called directly, instead of generating an
        # Expression object

        if inspect.getmodule(sys._getframe(1)) is pipda.function:
            # called directly
            return func(*args, **kwargs)
        _data, _column, *args = args
        series = evaluate_expr(_column, _data, context)
        args = evaluate_args(args, _data, context.args)
        kwargs = evaluate_kwargs(kwargs, _data, context.kwargs)
        return func(series, *args, **kwargs)

    @wrapper.register(DataFrameGroupBy)
    def _(
            _data: DataFrameGroupBy,
            _column: Any,
            *args: Any,
            **kwargs: Any
    ) -> Any:
        series = evaluate_expr(_column, _data, context)
        args = evaluate_args(args, _data, context.args)
        kwargs = evaluate_kwargs(kwargs, _data, context.kwargs)
        # Todo: check if we have SeriesGroupby in args/kwargs
        return series.apply(func, *args, **kwargs)

    return wrapper

def register_grouped(
        func: Optional[Callable] = None,
        context: Optional[Union[Context, ContextBase]] = None,
        columns: Union[str, int] = 1
) -> Callable:
    """Register a function as a group-by-aware function"""
    if func is None:
        return lambda fun: register_grouped(
            fun,
            context=context,
            columns=columns
        )

    if isinstance(context, Context):
        context = context.value

    if columns == 1:
        return _register_grouped_col1(func, context=context)

    if columns == 0:
        return _register_grouped_col0(func, context=context)

    raise ValueError("Expect columns to be either 0 or 1.")
