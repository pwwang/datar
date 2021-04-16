"""Functions from R-dplyr"""
import re
from typing import Any, Callable, Iterable, List, Mapping, Optional, Union

import numpy
import pandas
from pandas.core.arrays.categorical import Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from pandas import DataFrame, Series
from pandas.core.groupby.generic import DataFrameGroupBy

from pipda import register_func
from pipda.context import ContextBase
from pipda.utils import functype

from ..core.middlewares import (
    Across, CurColumn, DescSeries, IfAll, IfAny
)
from ..core.types import (
    BoolOrIter, DataFrameType, NumericOrIter, NumericType,
    is_iterable, is_scalar
)
from ..core.exceptions import ColumnNotExistingError
from ..core.utils import (
    copy_flags, filter_columns, list_union,
    objectize, list_diff, vars_select
)
from ..core.contexts import Context
from ..base.constants import NA

# pylint: disable=redefined-outer-name



@register_func(context=Context.SELECT)
def starts_with(
        _data: DataFrameType,
        match: Union[Iterable[str], str],
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None # pylint: disable=redefined-builtin
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        lambda mat, cname: cname.startswith(mat),
    )

@register_func(context=Context.SELECT)
def ends_with(
        _data: DataFrameType,
        match: str,
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None # pylint: disable=redefined-builtin
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        lambda mat, cname: cname.endswith(mat),
    )


@register_func(context=Context.SELECT)
def contains(
        _data: DataFrameType,
        match: str,
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None # pylint: disable=redefined-builtin
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        lambda mat, cname: mat in cname,
    )

@register_func(context=Context.SELECT)
def matches(
        _data: DataFrameType,
        match: str,
        ignore_case: bool = True,
        vars: Optional[Iterable[str]] = None # pylint: disable=redefined-builtin
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        re.search,
    )


@register_func(context=Context.EVAL)
def all_of(
        _data: DataFrameType,
        x: Iterable[Union[int, str]]
) -> List[str]:
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
    nonexists = set(x) - set(objectize(_data).columns)
    if nonexists:
        nonexists = ', '.join(f'`{elem}`' for elem in nonexists)
        raise ColumnNotExistingError(
            "Can't subset columns that don't exist. "
            f"Column(s) {nonexists} not exist."
        )

    return list(x)

@register_func(context=Context.SELECT)
def any_of(
        _data: DataFrameType,
        x: Iterable[Union[int, str]],
        vars: Optional[Iterable[str]] = None # pylint: disable=redefined-builtin
) -> List[str]:
    """Select but doesn't check for missing variables.

    It is especially useful with negative selections,
    when you would like to make sure a variable is removed.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns

    Returns:
        The matched column names
    """
    vars = vars or objectize(_data).columns
    return [elem for elem in x if elem in vars]






@register_func(None, context=Context.EVAL)
def between(
        x: NumericOrIter,
        left: NumericType,
        right: NumericType
) -> BoolOrIter:
    """Function version of `left <= x <= right`, which cannot do it rowwisely
    """
    if is_scalar(x):
        return left <= x <= right
    return Series(between(elem, left, right) for elem in x)

@register_func(None, context=Context.EVAL)
def n_distinct(series: Iterable[Any]) -> int:
    """Get the length of distince elements"""
    return len(set(series))




@register_func(None, context=Context.EVAL)
def cummean(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative means"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cumsum(skipna=False) / (Series(range(len(series))) + 1.0)

@register_func(None, context=Context.EVAL)
def cumall(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative bool. All cases after first False"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummin(skipna=False).astype(bool)

@register_func(None, context=Context.EVAL)
def cumany(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative bool. All cases after first True"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummax(skipna=False).astype(bool)

@register_func(None, context=Context.EVAL)
def lead(
        series: Iterable[Any],
        n: bool = 1,
        default: Any = NA,
        order_by: Optional[Iterable[NumericType]] = None
) -> Series:
    """Find next values in a vector

    Args:
        series: Vector of values
        n: Positive integer of length 1, giving the number of positions to
            lead or lag by
        default: Value used for non-existent rows.
        order_by: Override the default ordering to use another vector or column

    Returns:
        Lead or lag values with default values filled to series.
    """
    if not isinstance(series, Series):
        series = Series(series)

    index = series.index
    if order_by is not None:
        if not isinstance(order_by, Series):
            order_by = Series(order_by)
        order_by = order_by.sort_values(
            ascending=not isinstance(order_by, DescSeries)
        )
        series = series.loc[order_by.index]

    ret = [default] * len(series)
    ret[:-n] = series.values[n:]
    if order_by is not None:
        ret = Series(ret, index=order_by.index)
        return ret.loc[index]
    return Series(ret, index=index)

@register_func(None, context=Context.EVAL)
def lag(
        series: Iterable[Any],
        n: bool = 1,
        default: Any = NA,
        order_by: Optional[Iterable[NumericType]] = None
) -> Series:
    """Find previous values in a vector

    See lead()
    """
    if not isinstance(series, Series):
        series = Series(series)

    index = series.index
    if order_by is not None:
        if not isinstance(order_by, Series):
            order_by = Series(order_by)
        order_by = order_by.sort_values(
            ascending=not isinstance(order_by, DescSeries)
        )
        series = series.loc[order_by.index]

    ret = [default] * len(series)
    ret[n:] = series.values[:-n]
    if order_by is not None:
        ret = Series(ret, index=order_by.index)
        return ret.loc[index]
    return Series(ret, index=index)

@register_func(None)
def num_range(
        prefix: str,
        range: Iterable[int], # pylint: disable=redefined-builtin
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

@register_func(None, context=Context.EVAL)
def recode(
        series: Iterable[Any],
        *args: Any,
        _default: Any = None,
        _missing: Any = NA,
        **kwargs: Any
) -> Iterable[Any]:
    """Recode a vector, replacing elements in it

    Args:
        series: A vector to modify
        *args, **kwargs: replacements
        _default: If supplied, all values not otherwise matched will be
            given this value. If not supplied and if the replacements are
            the same type as the original values in series, unmatched values
            are not changed. If not supplied and if the replacements are
            not compatible, unmatched values are replaced with NA.
        _missing: If supplied, any missing values in .x will be replaced
            by this value.

    Returns:
        The vector with values replaced
    """
    kwd_recodes = {}
    for i, arg in enumerate(args):
        if isinstance(arg, dict):
            kwd_recodes.update(arg)
        else:
            kwd_recodes[i] = arg

    kwd_recodes.update(kwargs)

    series = objectize(series)
    series = numpy.array(series) # copied
    ret = [_missing] * len(series)

    for elem in set(series):
        if pandas.isna(elem):
            continue
        replace = kwd_recodes.get(elem, _default)
        replace = elem if replace is None else replace

        for i, indicator in enumerate(series == elem):
            if not indicator:
                continue
            ret[i] = replace

    return ret

@register_func(None, context=Context.EVAL)
def recode_factor(
        series: Iterable[Any],
        *args: Any,
        _default: Any = None,
        _missing: Any = NA,
        _ordered: bool = False,
        **kwargs: Any
) -> Iterable[Any]:
    """Recode a factor

    see recode().
    """
    if not is_categorical_dtype(series):
        series = Categorical(series)
    else:
        _default = NA if _default is None else _default

    categories = recode(
        series,
        *args,
        _default=_default,
        _missing=_missing,
        **kwargs
    )
    cats = []
    for cat in categories:
        if pandas.isnull(cat):
            continue
        if cat not in cats:
            cats.append(cat)

    series = recode(
        series,
        *args,
        _default=_default,
        _missing=_missing,
        **kwargs
    )

    return Categorical(
        series,
        categories=cats,
        ordered=_ordered
    )

recode_categorical = recode_factor # pylint: disable=invalid-name

@register_func(None, context=Context.EVAL)
def coalesce(x: Any, *replace: Any) -> Any:
    """Replace missing values

    https://dplyr.tidyverse.org/reference/coalesce.html

    Args:
        x: The vector to replace
        replace: The replacement

    Returns:
        A vector the same length as the first argument with missing values
        replaced by the first non-missing value.
    """
    if not replace:
        return x

    x = objectize(x)
    if isinstance(x, DataFrame):
        y = x.copy()
        copy_flags(y, x)
        for repl in replace:
            x = y.combine_first(repl)
            copy_flags(x, y)
            y = x
        return y

    if is_iterable(x):
        x = Series(x)
        for repl in replace:
            x = x.combine_first(
                Series(repl if is_iterable(repl) else [repl] * len(x))
            )
        return x.values

    return replace[0] if numpy.isnan(x) else x

@register_func(None, context=Context.EVAL)
def na_if(x: Iterable[Any], y: Any) -> Iterable[Any]:
    """Convert an annoying value to NA

    Args:
        x: Vector to modify
        y: Value to replace with NA

    Returns:
        A vector with values replaced.
    """
    x = objectize(x)
    if is_scalar(x):
        x = [x]
    if not isinstance(x, Series):
        x = Series(x)

    y = objectize(y)
    if not isinstance(y, Series) and is_scalar(y):
        y = Series(y)
    if isinstance(y, Series):
        y = y.values

    x = x.to_frame(name='x')
    x.loc[x.x.values == y] = NA
    return x['x']

@register_func(None, context=Context.EVAL)
def near(x: Iterable[Any], y: Any, tol: float = 1e-8) -> Iterable[Any]:
    """Compare numbers with tolerance"""
    x = objectize(x)
    if is_scalar(x):
        x = [x]

    y = objectize(y)

    return numpy.isclose(x, y, atol=tol)

@register_func(None, context=Context.EVAL)
def nth(
        x: Iterable[Any],
        n: int,
        order_by: Optional[Iterable[Any]] = None,
        default: Any = NA
) -> Any:
    """Get the nth element of x"""
    x = numpy.array(x)
    if order_by is not None:
        order_by = numpy.array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[n]
    except IndexError:
        return default

@register_func(None, context=Context.EVAL)
def first(
        x: Iterable[Any],
        order_by: Optional[Iterable[Any]] = None,
        default: Any = NA
) -> Any:
    """Get the first element of x"""
    x = numpy.array(x)
    if order_by is not None:
        order_by = numpy.array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[0]
    except IndexError:
        return default

@register_func(None, context=Context.EVAL)
def last(
        x: Iterable[Any],
        order_by: Optional[Iterable[Any]] = None,
        default: Any = NA
) -> Any:
    """Get the last element of x"""
    x = numpy.array(x)
    if order_by is not None:
        order_by = numpy.array(order_by)
        x = x[order_by.argsort()]
    try:
        return x[-1]
    except IndexError:
        return default

def group_by_drop_default(data: DataFrameType) -> bool:
    """Get the groupby _drop attribute of dataframe"""
    return getattr(objectize(data).flags, 'groupby_drop', True)

def n_groups(data: DataFrameType) -> int:
    """Get the number of groups"""
    if isinstance(data, DataFrame):
        return 1

    # when dropna=False with NAs
    # https://github.com/pandas-dev/pandas/issues/35202
    # return len(data)
    return len(data.size())

def group_size(data: DataFrameType) -> List[int]:
    """Get the group sizes as a list of integers"""
    if isinstance(data, DataFrame):
        return data.shape[0]
    gsize = data.size()
    if isinstance(gsize, Series):
        return gsize.tolist()
    return gsize['size'].tolist()
