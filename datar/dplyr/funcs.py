"""Functions from R-dplyr"""

import numpy
import pandas
from pandas.core.arrays.categorical import Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from datar.core.utils import filter_columns, objectize
from typing import Any, Callable, Iterable, List, Mapping, Optional, Union
from pandas import DataFrame, Series
from pandas.core.groupby.generic import DataFrameGroupBy, SeriesGroupBy
from pipda import register_func, Context

from ..core.middlewares import Across, CAcross, CurColumn, DescSeries, IfAll, IfAny
from ..core.types import DataFrameType, NumericType, is_scalar
from ..core.exceptions import ColumnNotExistingError
from ..core.utils import register_grouped
from ..base.constants import NA

@register_func((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def desc(
        _data: DataFrameType,
        col: str
) -> Union[DescSeries, SeriesGroupBy]:
    """Returns a DescSeries object, which can be used in arrange or other
    environments that need a descending ordered series

    Args:
        col: The column

    Returns:
        The DescSeries object
    """
    if isinstance(_data, DataFrameGroupBy):
        series = DescSeries(_data[col].obj.values, name=col)
        return series.groupby(_data.grouper, dropna=False)
    return DescSeries(_data[col].values, name=col)

@register_func(context=Context.SELECT)
def across(
        _data: DataFrameType,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[
            Callable,
            Iterable[Callable],
            Mapping[str, Callable]
        ]] = None,
        _names: Optional[str] = None,
        *args: Any,
        **kwargs: Any
) -> Across:
    """Apply the same transformation to multiple columns

    Args:
        _data: The dataframe
        _cols: The columns
        _fns: Functions to apply to each of the selected columns.
        _names: A glue specification that describes how to name
            the output columns. This can use `{_col}` to stand for the
            selected column name, and `{_fn}` to stand for the name of
            the function being applied.
            The default (None) is equivalent to `{_col}` for the
            single function case and `{_col}_{_fn}` for the case where
            a list is used for _fns. In such a case, `{_fn}` is 1-based.
            To use 1-based index, use `{_fn0}`
        *args, **kwargs: Arguments for the functions

    Returns:
        A dataframe with one column for each column in _cols and
        each function in _fns.
    """
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
    """Apply the same transformation to multiple columns rowwisely

    Args:
        _data: The dataframe
        _cols: The columns
        _fns: Functions to apply to each of the selected columns.
        _names: A glue specification that describes how to name
            the output columns. This can use `{_col}` to stand for the
            selected column name, and `{_fn}` to stand for the name of
            the function being applied.
            The default (None) is equivalent to `{_col}` for the
            single function case and `{_col}_{_fn}` for the case where
            a list is used for _fns. In such a case, `{_fn}` is 1-based.
            To use 1-based index, use `{_fn0}`
        *args, **kwargs: Arguments for the functions

    Returns:
        A dataframe with one column for each column in _cols and
        each function in _fns.
    """
    return CAcross(_data, _cols, _fns, _names, args, kwargs)


@register_func
def starts_with(
        _data: DataFrameType,
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        lambda mat, cname: cname.startswith(mat),
    )

@register_func
def ends_with(
        _data: DataFrameType,
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        lambda mat, cname: cname.endswith(mat),
    )


@register_func
def contains(
        _data: DataFrameType,
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        lambda mat, cname: mat in cname,
    )

@register_func
def matches(
        _data: DataFrameType,
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
        vars or objectize(_data).columns,
        match,
        ignore_case,
        lambda mat, cname: re.search(mat, cname),
    )

@register_func
def everything(_data: DataFrameType) -> List[str]:
    """Matches all columns.

    Args:
        _data: The data piped in

    Returns:
        All column names of _data
    """
    if isinstance(_data, DataFrameGroupBy):
        return list_diff(_data.obj.columns.tolist(), _data.grouper.names)
    return _data.columns.to_list()

@register_func
def last_col(
        _data: DataFrameType,
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

@register_func
def any_of(_data: DataFrameType,
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
    vars = vars or objectize(_data).columns
    return [elem for elem in x if elem in vars]

@register_func((DataFrame, DataFrameGroupBy))
def where(_data: DataFrameType, fn: Callable) -> List[str]:
    """Selects the variables for which a function returns True.

    Args:
        _data: The data piped in
        fn: A function that returns True or False.
            Currently it has to be `register_func/register_cfunction
            registered function purrr-like formula not supported yet.

    Returns:
        The matched columns
    """
    _data = objectize(_data)
    retcols = []

    pipda_type = getattr(fn, '__pipda__', None)
    for col in _data.columns:
        if not pipda_type:
            conditions = fn(_data[col])
        else:
            conditions = (
                fn(_data[col], _calling_type='piping').evaluate(_data)
                if pipda_type == 'PlainFunction'
                else fn(
                    _data,
                    _data[col],
                    _calling_type='piping'
                ).evaluate(_data)
            )
        if isinstance(conditions, bool):
            if conditions:
                retcols.append(col)
            else:
                continue
        elif all(conditions):
            retcols.append(col)

    return retcols

@register_func(context=Context.SELECT)
def if_any(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        *args: Any,
        **kwargs: Any
) -> Across:
    """apply the same predicate function to a selection of columns and combine
    the results True if any element is True.

    See across().
    """
    return IfAny(_data, _cols, _fns, _names, args, kwargs)


@register_func(context=Context.SELECT)
def if_all(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        *args: Any,
        **kwargs: Any
) -> Across:
    """apply the same predicate function to a selection of columns and combine
    the results True if all elements are True.

    See across().
    """
    return IfAll(_data, _cols, _fns, _names, args, kwargs)

def _ranking(
        data: Iterable[Any],
        na_last: str,
        method: str,
        percent: bool = False
) -> Iterable[float]:
    """Rank the data"""
    if not isinstance(data, Series):
        data = Series(data)

    ascending = not isinstance(data, DescSeries)

    ret = data.rank(
        method=method,
        ascending=ascending,
        pct=percent,
        na_option=(
            'keep' if na_last == 'keep'
            else 'top' if not na_last
            else 'bottom'
        )
    )
    return ret

@register_grouped(context=Context.EVAL)
def min_rank(series: Iterable[Any], na_last: str = "keep") -> Iterable[float]:
    """Rank the data using min method"""
    return _ranking(series, na_last=na_last, method='min')

@register_grouped(context=Context.EVAL)
def dense_rank(series: Iterable[Any], na_last: str = "keep") -> Iterable[float]:
    """Rank the data using dense method"""
    return _ranking(series, na_last=na_last, method='dense')

@register_grouped(context=Context.EVAL)
def percent_rank(
        series: Iterable[Any],
        na_last: str = "keep"
) -> Iterable[float]:
    """Rank the data using percent_rank method"""
    ranking = _ranking(series, na_last, 'min', True)
    min_rank = ranking.min()
    max_rank = ranking.max()
    ret = ranking.transform(lambda r: (r-min_rank)/(max_rank-min_rank))
    ret[ranking.isna()] = numpy.nan
    return ret


@register_grouped(context=Context.EVAL)
def cume_dist(series: Iterable[Any], na_last: str = "keep") -> Iterable[float]:
    """Rank the data using percent_rank method"""
    ranking = _ranking(series, na_last, 'min')
    max_ranking = ranking.max()
    ret = ranking.transform(lambda r: ranking.le(r).sum() / max_ranking)
    ret[ranking.isna()] = numpy.nan
    return ret

@register_grouped(context=Context.EVAL)
def ntile(series: Iterable[Any], n: int) -> Iterable[Any]:
    """A rough rank, which breaks the input vector into ‘n’ buckets."""
    return pandas.cut(series, n, labels=range(n))


@register_func((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def case_when(
        _data: DataFrameType,
        *when_cases: Any
) -> Series:
    """Vectorise multiple if_else() statements.

    Args:
        *when_cases: A even-size sequence, with 2n-th element values to match,
            and 2(n+1)-th element the values to replace.
            When matching value is True, then next value will be default to
            replace

    Returns:
        A series with values replaced
    """
    if len(when_cases) % 2 != 0:
        raise ValueError('Number of arguments of case_when should be even.')

    nrow = objectize(_data).shape[0]
    df = DataFrame({'x': [numpy.nan] * nrow})
    when_cases = reversed(list(zip(when_cases[0::2], when_cases[1::2])))
    for case, ret in when_cases:
        if case is True:
            df['x'] = ret
        else:
            df.loc[case, 'x'] = ret

    return df.x

@register_func((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def if_else(
        _data: DataFrameType,
        condition: Union[bool, Iterable[bool]],
        true: Any,
        false: Any,
        missing: Any = None
) -> Series:
    """Where condition is TRUE, the matching value from true, where it's FALSE,
    the matching value from false, otherwise missing.

    Args:
        condition: the conditions
        true, false: Values to use for TRUE and FALSE values of condition.
            They must be either the same length as condition, or length 1.
        missing: If not None, will be used to replace missing values

    Returns:
        A series with values replaced.
    """
    return case_when(
        _data,
        numpy.invert(condition), false,
        condition, true,
        True, missing
    )

@register_grouped(context=Context.EVAL)
def n_distinct(series: Iterable[Any]) -> int:
    """Get the length of distince elements"""
    return len(set(series))

@register_grouped(context=Context.EVAL, columns=0)
def n(series: Iterable[Any]) -> int:
    """gives the current group size."""
    return len(series)

@register_grouped(context=Context.EVAL, columns=0)
def row_number(series: Iterable[Any]) -> Iterable[int]:
    """Gives the row number, 0-based."""
    if isinstance(series, Series):
        return Series(range(len(series)))
    return series.cumcount()

@register_func(DataFrameGroupBy)
def cur_group_id(_data: DataFrameGroupBy) -> int:
    """gives a unique numeric identifier for the current group."""
    groups = [
        group_indexes.tolist()
        for group_indexes in _data.grouper.groups.values()
    ]
    return _data.apply(lambda df: groups.index(df.index.tolist()))

@register_func(DataFrameGroupBy)
def cur_group_rows(_data: DataFrameGroupBy) -> int:
    """gives the row indices for the current group."""
    return _data.apply(lambda df: df.index.tolist())

@register_func(DataFrameGroupBy)
def cur_group(_data: DataFrameGroupBy) -> Series:
    """gives the group keys, a tibble with one row and one column for
    each grouping variable."""
    ret = []
    keys = _data.grouper.names
    for key in _data.groups:
        if len(keys) == 1:
            ret.append(DataFrame([key], columns=keys))
        else:
            ret.append(DataFrame(zip(*key), columns=keys))

    return Series(ret)

@register_func(DataFrameGroupBy)
def cur_data(_data: DataFrameGroupBy) -> int:
    """gives the current data for the current group
    (excluding grouping variables)."""
    return Series(
        _data.obj.loc[index].drop(columns=_data.grouper.names)
        for index in _data.grouper.groups.values()
    )

@register_func(DataFrameGroupBy)
def cur_data_all(_data: DataFrameGroupBy) -> int:
    """gives the current data for the current group
    (including grouping variables)"""
    return Series(
        _data.obj.loc[index]
        for index in _data.grouper.groups.values()
    )

def cur_column() -> CurColumn:
    """Used in the functions of across. So we don't have to register it."""
    return CurColumn()

@register_grouped(context=Context.EVAL)
def cummean(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative means"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cumsum(skipna=False) / (Series(range(len(series))) + 1.0)

@register_grouped(context=Context.EVAL)
def cumall(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative bool. All cases after first False"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummin(skipna=False).astype(bool)

@register_grouped(context=Context.EVAL)
def cumany(series: Iterable[NumericType]) -> Iterable[float]:
    """Get cumulative bool. All cases after first True"""
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummax(skipna=False).astype(bool)

@register_grouped(context=Context.EVAL)
def lead(
        series: Iterable[Any],
        n: bool = 1,
        default = NA,
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

@register_grouped(context=Context.EVAL)
def lag(
        series: Iterable[Any],
        n: bool = 1,
        default = numpy.nan,
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

recode_categorical = recode_factor

@register_func(None, context=Context.EVAL)
def coalesce(x: Any, replace: Any) -> Any:
    """Replace missing values

    https://dplyr.tidyverse.org/reference/coalesce.html

    Args:
        x: The vector to replace
        replace: The replacement

    Returns:
        A vector the same length as the first argument with missing values
        replaced by the first non-missing value.
    """
    x = objectize(x)
    if isinstance(x, Iterable):
        if not isinstance(replace, Iterable):
            replace = [replace] * len(x)
        elif len(replace) != len(x):
            raise ValueError(
                f"Expect length {len(x)} for coalesce replacement, "
                f"got {len(replace)}"
            )
        return [
            rep if numpy.isnan(elem) else elem
            for elem, rep in zip(x, replace)
        ]

    return replace if numpy.isnan(x) else x

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
    x.loc[x.x.values==y] = numpy.nan
    return x['x']

@register_func(None, context=Context.EVAL)
def near(x: Iterable[Any], y: Any) -> Iterable[Any]:
    """Compare numbers with tolerance"""
    x = objectize(x)
    if is_scalar(x):
        x = [x]

    y = objectize(y)

    return numpy.isclose(x, y)

@register_grouped(context=Context.EVAL)
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

@register_grouped(context=Context.EVAL)
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

@register_grouped(context=Context.EVAL)
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
