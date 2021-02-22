import datetime
from functools import wraps
import functools
from numpy.core.numeric import NaN
from pandas.core.series import Series

from pipda.context import ContextBase, ContextSelect
from plyrda.verbs import select
import numpy
from plyrda.group_by import get_groups, get_rowwise
import re
from typing import Any, Callable, Iterable, Iterator, List, Mapping, Optional, Union

from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy
from pipda import register_func, Context

from .utils import filter_columns, select_columns
from .middlewares import Across, CAcross, Collection, RowwiseDataFrame, Inverted
from .exceptions import ColumnNotExistingError

DateType = Union[int, str, datetime.date]

def _register_datafunc_coln(
        func: Callable,
        context: Union[Context, ContextBase] = Context.EVAL
) -> Callable:

    @register_func(DataFrame, context=context)
    @wraps(func)
    def wrapper(_data: DataFrame, *columns, **kwargs) -> Any:

        if isinstance(_data, RowwiseDataFrame):
            return  func(*(row for row in zip(*columns)), **kwargs)
        # flatten
        return func(*columns, **kwargs)

    return wrapper

def _register_datafunc_col1(
        func: Callable,
        context: Union[Context, ContextBase] = Context.EVAL
) -> Callable:

    @register_func(DataFrame, context=context)
    @wraps(func)
    def wrapper(_data: DataFrame, column, *args, **kwargs):

        if isinstance(_data, RowwiseDataFrame):
            return _data.apply(
                lambda row: func([row[col] for col in columns], *args, **kwargs)
            )

        return func(_data[columns].values.flatten(), *args, **kwargs)

    @wrapper.register(DataFrameGroupBy)
    def _(_data: DataFrameGroupBy, column, *args, **kwargs: Any) -> Any:
        column = select_columns(_data.obj.columns, column)
        return _data[column].apply(func, *args, **kwargs)

    return wrapper

def register_datafunc(func=None, columns=1):
    if func is None:
        return lambda fun: register_datafunc(fun, columns=columns)

    if columns == '*':
        return _register_datafunc_coln(func)

    if columns == 1:
        return _register_datafunc_col1(func)

    raise ValueError("Expect columns to be either '*', or 1.")

@register_func
def starts_with(
        _data: DataFrame,
        match: Union[Iterable[str], str],
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None,
) -> List[str]:
    """Select columns starting with a prefix.

    Args:
        _data: The data piped in
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        lambda mat, cname: cname.startswith(mat),
    )

@register_func
def ends_with(
        _data: DataFrame,
        match: str,
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None,
) -> List[str]:
    """Select columns ending with a suffix.

    Args:
        _data: The data piped in
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        lambda mat, cname: cname.endswith(mat),
    )

@register_func
def contains(
        _data: DataFrame,
        match: str,
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None,
) -> List[str]:
    """Select columns containing substrings.

    Args:
        _data: The data piped in
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        lambda mat, cname: mat in cname,
    )

@register_func
def matches(
        _data: DataFrame,
        match: str,
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None,
) -> List[str]:
    """Select columns matching regular expressions.

    Args:
        _data: The data piped in
        match: Regular expressions. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        lambda mat, cname: re.search(mat, cname),
    )

@register_func
def everything(_data: DataFrame) -> List[str]:
    """Matches all columns.

    Args:
        _data: The data piped in

    Returns:
        All column names of _data
    """
    return _data.columns.to_list()

@register_func
def last_col(
        _data: DataFrame,
        offset: int = 0,
        vars: Optional[Iterable[str]] = None
) -> str:
    """Select last variable, possibly with an offset.

    Args:
        _data: The data piped in
        offset: The offset from the end.
            Note that this is 0-based, the same as `tidyverse`'s `last_col`
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        The variable
    """
    vars = vars or _data.columns
    return vars[-(offset+1)]

@register_func
def all_of(_data: DataFrame, x: Iterable[Union[int, str]]) -> List[str]:
    """For strict selection.

    If any of the variables in the character vector is missing,
    an error is thrown.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns

    Returns:
        The matched column names

    Raises:
        ColumnNotExistingError: When any of the elements in `x` does not exist
            in `_data` columns
    """
    nonexists = set(x) - set(_data.columns)
    if nonexists:
        nonexists = ', '.join(f'`{elem}`' for elem in nonexists)
        raise ColumnNotExistingError(
            "Can't subset columns that don't exist. "
            f"Column(s) {nonexists} not exist."
        )

    return list(x)

@register_func
def any_of(_data: DataFrame,
           x: Iterable[Union[int, str]],
           vars: Optional[Iterable[str]] = None) -> List[str]:
    """Select but doesn't check for missing variables.

    It is especially useful with negative selections,
    when you would like to make sure a variable is removed.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns

    Returns:
        The matched column names
    """
    vars = vars or _data.columns
    return [elem for elem in x if elem in vars]

@register_func
def where(_data: DataFrame, fn: Callable) -> List[str]:
    """Selects the variables for which a function returns True.

    Args:
        _data: The data piped in
        fn: A function that returns True or False.
            Currently it has to be `register_func/register_cfunction
            registered function purrr-like formula not supported yet.

    Returns:
        The matched columns
    """
    if not hasattr(fn, '__pipda__'):
        func = lambda _data, *args, **kwargs: fn(*args, **kwargs)
    elif fn.__pipda__ == 'CommonFunction':
        func = lambda _data, *args, **kwargs: fn(
            *args, **kwargs, _force_piping=True
        ).evaluate(_data)
    else:
        func = lambda _data, *args, **kwargs: fn(
            *args, **kwargs, _force_piping=True
        ).evaluate(_data)

    return [col for col in _data.columns if func(_data, _data[col])]

@register_func
def desc(_data, col):
    return Inverted(col, data=_data)

@register_func(context=Context.SELECT)
def across(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        *args: Any,
        **kwargs: Any
) -> Across:
    return Across(_data, _cols, _fns, _names, args, kwargs)

@register_func(context=Context.SELECT)
def c_across(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        *args: Any,
        **kwargs: Any
) -> CAcross:
    return CAcross(_data, _cols, _fns, _names, args, kwargs)

def _ranking(data: Iterable[Any], na_last: str, method: str, percent: bool = False) -> Iterable[float]:
    groups = get_groups(data)
    if groups:
        _data = data.groupby(groups)

    if isinstance(column, Inverted):
        ascending = False
        _data = _data[column.elems[0]]
    else:
        ascending = True
        _data = _data[column]

    return _data.rank(
        method=method,
        ascending=ascending,
        pct=percent,
        na_option=('keep' if na_last == 'keep'
                   else 'top' if not na_last
                   else 'bottom')
    )

@register_datafunc
def min_rank(_data, column, na_last="keep"):
    return _ranking(_data, column, na_last=na_last, method='min')


@register_datafunc
def sum(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nansum(x) if na_rm else numpy.sum(x)

@register_datafunc
def mean(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nanmean(x) if na_rm else numpy.mean(x)

@register_datafunc
def min(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nanmin(x) if na_rm else numpy.min(x)

@register_datafunc
def max(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nanmax(x) if na_rm else numpy.max(x)

@register_func
def pmin(*x: Union[int, float], na_rm: bool = False) -> Iterable[float]:
    return [min(elem, na_rm) for elem in zip(*x)]

@register_func
def pmax(*x: Union[int, float], na_rm: bool = False) -> Iterable[float]:
    return [max(elem, na_rm) for elem in zip(*x)]

@register_datafunc
def sd(
        x: Iterable[Union[int, float]],
        na_rm: bool = False,
        ddof: int = 1 # numpy default is 0. Make it 1 to be consistent with R
) -> float:
    return numpy.nanstd(x, ddof=ddof) if na_rm else numpy.std(x, ddof=ddof)

@register_datafunc(columns='*')
def n(x: Optional[Iterable[Any]] = None) -> int:
    return len(x)


# Functions without data arguments
# --------------------------------

def register_vectorized(func):
    """Vectorize the common functions

    Note that only the first argument is vectorized.

    Args:
        func: The function to be vectorized
    """


    @register_func
    @functools.wraps(func)
    def wrapper(x, *args, **kwargs):
        partial_func = lambda y: func(y, *args, **kwargs)
        vec_func = vectorize(partial_func)
        return vec_func(x)
    return wrapper

@register_func(None, context=Context.SELECT)
def c(*elems: Any) -> Collection:
    """Mimic R's concatenation. Named one is not supported yet
    All elements passed in will be flattened.

    Args:
        _data: The data piped in
        *elems: The elements

    Returns:
        A collection of elements
    """
    return Collection(elems)

@register_vectorized
def is_numeric(x: Any) -> bool:
    return isinstance(x, (int, float))

@register_vectorized
def is_character(x: Any) -> bool:
    """Mimic the is.character function in R

    Args:
        x: The elements to check

    Returns:
        True if
    """
    return isinstance(x, str)

@register_func
def is_categorical(x: Series) -> bool:
    return isinstance(x.dtype, CategoricalDtype)

@register_func(None)
def as_categorical(x: Series) -> Series:
    return x.astype('category')

@register_vectorized
def as_character(x: Any) -> str:
    return str(x)


@register_func
def num_range(
        prefix: str,
        range: Iterable[int],
        width: Optional[int] = None
) -> List[str]:
    """Matches a numerical range like x01, x02, x03.

    Args:
        _data: The data piped in
        prefix: A prefix that starts the numeric range.
        range: A sequence of integers, like `range(3)` (produces `0,1,2`).
        width: Optionally, the "width" of the numeric range.
            For example, a range of 2 gives "01", a range of three "001", etc.

    Returns:
        A list of ranges with prefix.
    """
    return [
        f"{prefix}{elem if not width else str(elem).zfill(width)}"
        for elem in range
    ]

# todo: figure out singledispatch for as_date
def _as_date_format(
        x: str,
        format: Optional[str],
        try_formats: Optional[Iterator[str]],
        optional: bool,
        offset: datetime.timedelta
) -> datetime.date:
    try_formats = try_formats or [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S"
    ]
    if not format:
        format = try_formats
    else:
        format = [format]

    for fmt in format:
        try:
            return (datetime.datetime.strptime(x, fmt) + offset).date()
        except ValueError:
            continue
    else:
        if optional:
            return NaN
        else:
            raise ValueError(
                "character string is not in a standard unambiguous format"
            )

def _as_date_diff(
        x: int,
        origin: Union[DateType, datetime.datetime],
        offset: datetime.timedelta
) -> datetime.date:
    if isinstance(origin, str):
        origin = as_date(origin)

    dt = origin + datetime.timedelta(days=x) + offset
    if isinstance(dt, datetime.date):
        return dt

    return dt.date()

@register_vectorized
def as_date(
        x: DateType,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
        origin: Optional[Union[DateType, datetime.datetime]] = None
):
    """Convert an object to a datetime.date object

    See: https://rdrr.io/r/base/as.Date.html

    Args:
        x: Object that can be converted into a datetime.date object
        format:  If not specified, it will try try_formats one by one on
            the first non-NaN element, and give an error if none works.
            Otherwise, the processing is via strptime
        try_formats: vector of format strings to try if format is not specified.
        optional: indicating to return NA (instead of signalling an error)
            if the format guessing does not succeed.
        origin: a datetime.date/datetime object, or something which can be
            coerced by as_date(origin, ...) to such an object.
        tz: a time zone offset or a datetime.timedelta object.
            Note that time zone name is not supported yet.

    Returns:
        The datetime.date object

    Raises:
        ValueError: When string is not in a standard unambiguous format
    """
    if isinstance(tz, (int, numpy.integer)):
        tz = datetime.timedelta(hours=int(tz))

    if isinstance(x, datetime.date):
        return x + tz

    if isinstance(x, datetime.datetime):
        return (x + tz).date()

    if isinstance(x, str):
        return _as_date_format(
            x,
            format=format,
            try_formats=try_formats,
            optional=optional,
            offset=tz
        )

    if isinstance(x, (int, numpy.integer)):
        return _as_date_diff(int(x), origin=origin, offset=tz)

    raise ValueError("character string is not in a standard unambiguous format")

