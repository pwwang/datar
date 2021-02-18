from plyrda.verbs import select
import numpy
from plyrda.group_by import get_groups, get_rowwise
import re
from typing import Any, Callable, Iterable, List, Mapping, Optional, Union

from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy
from pipda import register_function, Context

from .utils import filter_columns, select_columns
from .middlewares import Across, CAcross, UnaryNeg
from .exceptions import ColumnNotExistingError

def _register_datafunc_coln(func: Callable) -> Callable:

    @register_function(DataFrame)
    def wrapper(_data: DataFrame, *columns, **kwargs):
        columns = select_columns(_data.columns, *columns)
        if get_rowwise(_data):
            return _data.apply(
                lambda row: func(*(row[col] for col in columns), **kwargs)
            )

        groups = get_groups(_data)
        if groups:
            return _data.groupby(groups)[columns].apply(func, **kwargs)

        return func(*_data[columns].values.flatten(), **kwargs)

    return wrapper

def _register_datafunc_col1(func: Callable) -> Callable:

    @register_function(DataFrame)
    def wrapper(_data: DataFrame, column, *args, **kwargs):
        columns = select_columns(_data.columns, column)
        if get_rowwise(_data):
            return _data.apply(
                lambda row: func([row[col] for col in columns], *args, **kwargs)
            )

        groups = get_groups(_data)
        if groups:
            return _data.groupby(groups)[columns].apply(func, **kwargs)

        return func(_data[columns].values.flatten(), **kwargs)

    return wrapper

def register_datafunc(func=None, columns='*'):
    if func is None:
        return lambda fun: register_datafunc(fun, columns=columns)

    if columns == '*':
        return _register_datafunc_coln(func)

    if columns == 1:
        return _register_datafunc_col1(func)

    raise ValueError("Expect columns to be either '*', or 1.")

@register_function
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

@register_function
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

@register_function
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

@register_function
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

@register_function
def everything(_data: DataFrame) -> List[str]:
    """Matches all columns.

    Args:
        _data: The data piped in

    Returns:
        All column names of _data
    """
    return _data.columns.to_list()

@register_function
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

@register_function
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

@register_function
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

@register_function
def where(_data: DataFrame, fn: Callable) -> List[str]:
    """Selects the variables for which a function returns True.

    Args:
        _data: The data piped in
        fn: A function that returns True or False.
            Currently it has to be `register_function/register_cfunction
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

@register_function
def desc(_data, col):
    return UnaryNeg(col, data=_data)

@register_function
def across(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        *args: Any,
        **kwargs: Any
) -> Across:
    return Across(_data, _cols, _fns, _names, args, kwargs)

@register_function
def c_across(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        *args: Any,
        **kwargs: Any
) -> CAcross:
    return CAcross(_data, _cols, _fns, _names, args, kwargs)

def _ranking(_data, column, na_last, method, percent=False):
    groups = get_groups(_data)
    if groups:
        _data = _data.groupby(groups)

    if isinstance(column, UnaryNeg):
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

@register_datafunc(columns=1)
def min_rank(_data, column, na_last="keep"):
    return _ranking(_data, column, na_last=na_last, method='min')


@register_datafunc(columns=1)
def sum(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nansum(x) if na_rm else numpy.sum(x)

@register_datafunc(columns=1)
def mean(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nanmean(x) if na_rm else numpy.mean(x)

@register_datafunc(columns=1)
def min(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nanmin(x) if na_rm else numpy.min(x)

@register_datafunc(columns=1)
def max(x: Iterable[Union[int, float]], na_rm: bool = False) -> float:
    return numpy.nanmax(x) if na_rm else numpy.max(x)

@register_function
def pmin(*x: Union[int, float], na_rm: bool = False) -> Iterable[float]:
    return [min(elem, na_rm) for elem in zip(*x)]

@register_function
def pmax(*x: Union[int, float], na_rm: bool = False) -> Iterable[float]:
    return [max(elem, na_rm) for elem in zip(*x)]

@register_datafunc(columns=1)
def sd(
        x: Iterable[Union[int, float]],
        na_rm: bool = False,
        ddof: int = 1 # numpy default is 0. Make it 1 to be consistent with R
) -> float:
    return numpy.nanstd(x, ddof=ddof) if na_rm else numpy.std(x, ddof=ddof)
