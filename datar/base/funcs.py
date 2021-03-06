"""Some functions from R-base

If a function uses DataFrame/DataFrameGroupBy as first argument, it may be
registered by `register_verb` and should be placed in `./verbs.py`
"""
import builtins
import math
import datetime
import functools
from typing import Any, Iterable, List, Optional, Type, Union

import numpy
import pandas
from pandas.core.dtypes.common import (
    is_categorical_dtype, is_float_dtype, is_numeric_dtype, is_string_dtype
)
from pandas import Series, Categorical, DataFrame
from pandas.core.groupby.generic import SeriesGroupBy
from pipda import Context, register_func

from .constants import NA
from ..core.utils import categorize, objectize, register_grouped
from ..core.middlewares import Collection, ContextWithData
from ..core.types import (
    BoolOrIter, DataFrameType, DoubleOrIter, IntOrIter, NumericOrIter,
    NumericType, SeriesLikeType, StringOrIter, is_int, IntType,
    is_iterable, is_series_like, is_scalar
)

# pylint: disable=redefined-builtin,invalid-name

@functools.singledispatch
def _as_date_dummy(
        x: Any,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[IntType, datetime.timedelta] = 0,
        origin: Any = None
) -> datetime.date:
    """Convert a dummy object to date"""
    raise ValueError(f'Unable to convert to date with type: {type(x)!r}')

@_as_date_dummy.register(datetime.date)
def _(
        x: datetime.date,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
        origin: Any = None
) -> datetime.date:
    if is_int(tz):
        tz = datetime.timedelta(hours=int(tz))

    return x + tz

@_as_date_dummy.register(datetime.datetime)
def _(
        x: datetime.datetime,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[IntType, datetime.timedelta] = 0,
        origin: Any = None
):
    if is_int(tz):
        tz = datetime.timedelta(hours=int(tz))

    return (x + tz).date()

@_as_date_dummy.register(str)
def _(
        x: str,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[IntType, datetime.timedelta] = 0,
        origin: Any = None
) -> datetime.date:
    if is_int(tz):
        tz = datetime.timedelta(hours=int(tz))

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
            return (datetime.datetime.strptime(x, fmt) + tz).date()
        except ValueError:
            continue

    if optional:
        return numpy.nan

    raise ValueError(
        "character string is not in a standard unambiguous format"
    )

@register_grouped(context=Context.EVAL)
def sum(series: Iterable[Any], na_rm: bool = False) -> float:
    """Get the sum of input"""
    return numpy.nansum(series) if na_rm else numpy.sum(series)

@register_grouped(context=Context.EVAL)
def mean(series: Iterable[Any], na_rm: bool = False) -> float:
    """Get the mean of input"""
    return numpy.nanmean(series) if na_rm else numpy.mean(series)

@register_grouped(context=Context.EVAL)
def min(series: Iterable[Any], na_rm: bool = False) -> float:
    """Get the min of input"""
    return numpy.nanmin(series) if na_rm else numpy.min(series)

@register_grouped(context=Context.EVAL)
def max(series: Iterable[Any], na_rm: bool = False) -> float:
    """Get the max of input"""
    return numpy.nanmax(series) if na_rm else numpy.max(series)

@_as_date_dummy.register(Series)
def _(
        x: Series,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[IntType, datetime.timedelta] = 0,
        origin: Any = None
) -> datetime.date:
    return _as_date_dummy(
        x.values[0],
        format=format,
        try_formats=try_formats,
        optional=optional,
        tz=tz,
        origin=origin
    )

@_as_date_dummy.register(int)
@_as_date_dummy.register(numpy.integer)
def _(
        x: IntType,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[IntType, datetime.timedelta] = 0,
        origin: Any = None
) -> datetime.date:
    if isinstance(tz, (int, numpy.integer)):
        tz = datetime.timedelta(hours=int(tz))

    if isinstance(origin, str):
        origin = _as_date_dummy(origin)

    dt = origin + datetime.timedelta(days=int(x)) + tz

    if isinstance(dt, datetime.datetime):
        return dt.date()
    return dt

@register_func(None, context=Context.EVAL)
def as_date(
        x: DataFrameType,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[IntType, datetime.timedelta] = 0,
        origin: Any = None
) -> Iterable[datetime.date]:
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
    """
    if not isinstance(x, (Series, SeriesGroupBy)):
        x = Series([x]) if is_scalar(x) else Series(x)

    return objectize(x).transform(
        _as_date_dummy,
        format=format,
        try_formats=try_formats,
        optional=optional,
        tz=tz,
        origin=origin
    )

def _as_type(x: Any, type_: Type) -> Any:
    """Convert x or elements of x to certain type"""
    if is_series_like(x):
        x = objectize(x)
        return x.astype(type_)

    if is_scalar(x):
        return type_(x)

    return type(x)(map(type_, x))

@register_func(None, context=Context.EVAL)
def as_character(x: Any) -> StringOrIter:
    """Convert an object or elements of an iterable into string

    Args:
        x: The object

    Returns:
        When x is a numpy.ndarray or a series, return x.astype(str).
        When x is iterable, convert elements of it into strings
        Otherwise, convert x to string.
    """
    return _as_type(x, str)

@register_func(None, context=Context.EVAL)
def as_double(x: Any) -> DoubleOrIter:
    """Convert an object or elements of an iterable into double/float

    Args:
        x: The object

    Returns:
        When x is a numpy.ndarray or a series, return x.astype(float).
        When x is iterable, convert elements of it into floats
        Otherwise, convert x to float.
    """
    return _as_type(x, numpy.double)

@register_func(None, context=Context.EVAL)
def as_factor(x: Iterable[Any]) -> Categorical:
    """Convert an iterable into a pandas.Categorical object

    Args:
        x: The iterable

    Returns:
        The converted categorical object
    """
    x = objectize(x)
    return Categorical(x)

as_categorical = as_factor

@register_func(None, context=Context.EVAL)
def as_int(x: Any) -> Union[int, Iterable[int]]:
    """Convert an object or elements of an iterable into int

    Args:
        x: The object

    Returns:
        When x is a numpy.ndarray or a series, return x.astype(int).
        When x is iterable, convert elements of it into ints
        Otherwise, convert x to int.
    """
    if is_categorical_dtype(x):
        return categorize(x).codes
    return _as_type(x, int)

@register_func(None, context=Context.EVAL)
def as_integer(x: Any) -> Union[numpy.int64, Iterable[numpy.int64]]:
    """Convert an object or elements of an iterable into int64

    Args:
        x: The object

    Returns:
        When x is a numpy.ndarray or a series, return x.astype(numpy.int64).
        When x is iterable, convert elements of it into numpy.int64s
        Otherwise, convert x to numpy.int64.
    """
    if is_categorical_dtype(x):
        return categorize(x).codes
    return _as_type(x, numpy.int64)

as_int64 = as_integer

@register_func(None, context=Context.EVAL)
def as_logical(x: Any) -> BoolOrIter:
    """Convert an object or elements of an iterable into bool

    Args:
        x: The object

    Returns:
        When x is a numpy.ndarray or a series, return x.astype(bool).
        When x is iterable, convert elements of it into bools
        Otherwise, convert x to bool.
    """
    return _as_type(x, bool)

as_bool = as_logical

@register_func(None, context=Context.EVAL)
def is_numeric(x: Any) -> BoolOrIter:
    """Check if x is numeric type"""
    x = objectize(x)
    if is_scalar(x):
        return isinstance(x, int, float, complex, numpy.number)

    return is_numeric_dtype(x)

@register_func(None, context=Context.EVAL)
def is_character(x: Any) -> BoolOrIter:
    """Mimic the is.character function in R

    Args:
        x: The elements to check

    Returns:
        True if
    """
    x = objectize(x)
    if is_scalar(x):
        return isinstance(x, str)
    return is_string_dtype(x)

@register_func(None, context=Context.EVAL)
def is_categorical(x: Any) -> bool:
    """Check if x is categorical data"""
    x = objectize(x)
    return is_categorical_dtype(x)

is_factor = is_categorical

@register_func(None, context=Context.EVAL)
def is_double(x: Any) -> BoolOrIter:
    """Check if x is double/float data"""
    x = objectize(x)
    if is_scalar(x):
        return isinstance(x, (float, numpy.float))
    return is_float_dtype(x)

is_float = is_double

@register_func(None, context=Context.EVAL)
def is_na(x: Any) -> BoolOrIter:
    """Check if x is nan or not"""
    x = objectize(x)
    return numpy.isnan(x)

@register_func(None, context=Context.UNSET)
def c(*elems: Any) -> Collection:
    """Mimic R's concatenation. Named one is not supported yet
    All elements passed in will be flattened.

    Args:
        _data: The data piped in
        *elems: The elements

    Returns:
        A collection of elements
    """
    return Collection(*elems)

@register_func(None)
def seq_along(along_with: Iterable[Any]) -> SeriesLikeType:
    """Generate sequences along an iterable"""
    return numpy.array(range(len(along_with)))

@register_func(None)
def seq_len(length_out: int) -> SeriesLikeType:
    """Generate sequences with the length"""
    return numpy.array(range(length_out))

@register_func(None, context=Context.EVAL)
def seq(
        from_: IntType = None,
        to: IntType = None,
        by: IntType = None,
        length_out: IntType = None,
        along_with: IntType = None
) -> SeriesLikeType:
    """Generate a sequence"""
    if along_with is not None:
        return seq_along(along_with)
    if from_ is not None and not isinstance(from_, (int, float)):
        return seq_along(from_)
    if length_out is not None and from_ is None and to is None:
        return seq_len(length_out)

    if from_ is None:
        from_ = 0
    elif to is None:
        from_, to = 0, from_

    if length_out is not None:
        by = (float(to) - float(from_)) / float(length_out)
    elif by is None:
        by = 1
        length_out = to - from_
    else:
        length_out = (to - from_ + by - by/10.0) // by
    return numpy.array([from_ + n * by for n in range(int(length_out))])

@register_func(None, context=Context.EVAL)
def abs(x: Any) -> NumericOrIter:
    """Get the absolute value"""
    x = objectize(x)
    return numpy.abs(x)

@register_func(None, context=Context.EVAL)
def ceiling(x: NumericOrIter) -> IntOrIter:
    """Get the ceiling integer of x

    Args:
        x: The number

    Returns:
        The ceiling integer of x
    """
    if is_scalar(x):
        return math.ceil(x)
    return list(map(math.ceil, x))

@register_func(None, context=Context.EVAL)
def floor(x: NumericOrIter) -> IntOrIter:
    """Get the floor integer of x

    Args:
        x: The number

    Returns:
        The ceiling integer of x
    """
    if is_scalar(x):
        return math.floor(x)
    return list(map(math.floor, x))

@register_grouped(context=Context.EVAL)
def cumsum(
        series: Iterable[NumericType],
        skipna: bool = False
) -> SeriesLikeType:
    """Returns a vector whose elements are the cumulative sums of
    the elements of the argument.

    Args:
        series: A series of numbers
        skipna: Whether skipping NA's or not. Note that R doesn't have this
            argument.

    Returns:
        The cumulative sums
    """
    if not is_series_like(series):
        series = Series(series)
    return series.cumsum(skipna=skipna)

@register_grouped(context=Context.EVAL)
def cummin(
        series: Iterable[NumericType],
        skipna: bool = False
) -> SeriesLikeType:
    """Returns a vector whose elements are the cumulative minima of
    the elements of the argument.

    Args:
        series: A series of numbers
        skipna: Whether skipping NA's or not. Note that R doesn't have this
            argument.

    Returns:
        The cumulative minimas
    """
    if not is_series_like(series):
        series = Series(series)
    return series.cummin(skipna=skipna)

@register_grouped(context=Context.EVAL)
def cummax(
        series: Iterable[NumericType],
        skipna: bool = False
) -> SeriesLikeType:
    """Returns a vector whose elements are the cumulative maxima of
    the elements of the argument.

    Args:
        series: A series of numbers
        skipna: Whether skipping NA's or not. Note that R doesn't have this
            argument.

    Returns:
        The cumulative maximas
    """
    if not is_series_like(series):
        series = Series(series)
    return series.cummax(skipna=skipna)

@register_grouped(context=Context.EVAL)
def cumprod(
        series: Iterable[NumericType],
        skipna: bool = False
) -> SeriesLikeType:
    """Returns a vector whose elements are the cumulative products of
    the elements of the argument.

    Args:
        series: A series of numbers
        skipna: Whether skipping NA's or not. Note that R doesn't have this
            argument.

    Returns:
        The cumulative products
    """
    if not is_series_like(series):
        series = Series(series)
    return series.cumprod(skipna=skipna)

@register_grouped(context=Context.EVAL)
def cut(
        x: Iterable[NumericType],
        breaks: Any,
        labels: Optional[Iterable[Any]] = None,
        include_lowest: bool = False,
        right: bool = True,
        precision: int = 2,
        ordered_result: bool = False
) -> Categorical:
    """Divides the range of x into intervals and codes the values in x
    according to which interval they fall. The leftmost interval corresponds
    to level one, the next leftmost to level two and so on.

    Args:
        x: a numeric vector which is to be converted to a factor by cutting.
        breaks: either a numeric vector of two or more unique cut points or
            a single number (greater than or equal to 2) giving the number of
            intervals into which x is to be cut.
        labels: labels for the levels of the resulting category. By default,
            labels are constructed using "(a,b]" interval notation.
            If labels = False, simple integer codes are returned instead
            of a factor.
        include_lowest: bool, indicating if an ‘x[i]’ equal to the lowest
            (or highest, for right = FALSE) ‘breaks’ value should be included.
        right: bool, indicating if the intervals should be closed on the right
            (and open on the left) or vice versa.
        precision:integer which is used when labels are not given. It determines
            the precision used in formatting the break numbers. Note, this
            argument is different from R's API, which is dig.lab.
        ordered_result: bool, should the result be an ordered categorical?

    Returns:
        A categorical object with the cuts
    """
    if labels is None:
        ordered_result = True

    return pandas.cut(
        x,
        breaks,
        labels=labels,
        include_lowest=include_lowest,
        right=right,
        precision=precision,
        ordered=ordered_result
    )

@register_grouped(context=Context.EVAL)
def sample(
        x: Union[int, Iterable[Any]],
        size: Optional[int] = None,
        replace: bool = False,
        prob: Optional[Iterable[NumericType]] = None
) -> Iterable[Any]:
    """Takes a sample of the specified size from the elements of x using
    either with or without replacement.

    https://rdrr.io/r/base/sample.html

    Args:
        x: either a vector of one or more elements from which to choose,
            or a positive integer.
        n: a positive number, the number of items to choose from.
        size: a non-negative integer giving the number of items to choose.
        replace: should sampling be with replacement?
        prob: a vector of probability weights for obtaining the elements of
            the vector being sampled.

    Returns:
        A vector of length size with elements drawn from either x or from the
        integers 1:x.
    """
    if isinstance(x, str):
        x = list(x)
    if size is None:
        size = len(x) if is_iterable(x) else x
    return numpy.random.choice(x, int(size), replace=replace, p=prob)

@register_func(None, context=Context.EVAL)
def pmin(
        *series: Union[Series, SeriesGroupBy],
        na_rm: bool = False
) -> Iterable[float]:
    """Get the min value rowwisely"""
    series = (objectize(ser) for ser in series)
    return [min(elem, na_rm=na_rm) for elem in zip(*series)]

@register_func(None, context=Context.EVAL)
def pmax(
        *series: Union[Series, SeriesGroupBy],
        na_rm: bool = False
) -> Iterable[float]:
    """Get the max value rowwisely"""
    series = (objectize(ser) for ser in series)
    return [max(elem, na_rm=na_rm) for elem in zip(*series)]

@register_grouped(context=Context.EVAL)
def table(
        obj: Any,
        *objs: Any,
        exclude: Any = NA,
        # not supported. use exclude instead
        # use_na: str = "no",
        dnn: Optional[Union[str, List[str]]] = None,
        # not supported, varname.argname not working with wrappers having
        # different signatures.
        # deparse_level: int = 1
) -> DataFrame:
    # pylint: disable=too-many-statements,too-many-branches
    """uses the cross-classifying factors to build a contingency table of
    the counts at each combination of factor levels.

    When used with DataFrameGroupBy data, groups are ignored.

    Args:
        obj, *objs: one or more objects which can be interpreted as factors
            Only 1 or 2 variables allowed currently.
            If obj or elements of objs is a DataFrame, each column is counted
            as a variable.
        exclude: levels to remove for all factors
        dnn: the names to be given to the dimensions in the result.

    Returns:
        A contingency table (DataFrame)
    """
    obj1 = obj2 = None
    obj_nvar = 1
    if not isinstance(obj, DataFrame):
        obj1 = list(obj) if isinstance(obj, str) else obj
        obj_nvar = 1
    elif obj.shape[1] == 1:
        obj1 = obj.iloc[:, 0]
        obj_nvar = 1
    elif obj.shape[1] == 2:
        obj1 = obj.iloc[:, 0]
        obj2 = obj.iloc[:, 1]
        obj_nvar = 2
    else:
        raise ValueError(
            'At most 2 columns supported in the dataframe for `table`'
        )

    if obj_nvar == 2 and objs:
        raise ValueError(
            'At most 2 variables supported for `table`'
        )
    if objs:
        obj = objs[0]
        if isinstance(obj, DataFrame) and obj.shape[1] > 1:
            raise ValueError(
                'At most 2 variables supported for `table`'
            )
        if isinstance(obj, DataFrame):
            obj2 = obj.iloc[:, 0]
        elif isinstance(obj, str):
            obj2 = list(obj)
        else:
            obj2 = obj

    if obj2 is None:
        obj2 = obj1

    dn1 = dn2 = None
    if isinstance(dnn, str):
        dn1 = dn2 = dnn
    elif is_iterable(dnn):
        dnn = list(dnn)
        if len(dnn) == 1:
            dnn = dnn * 2
        dn1, dn2 = dnn[:2]

    if obj1 is obj2:
        if not isinstance(obj1, (Series, Categorical)):
            obj1 = obj2 = Series(obj1)
    else:
        if not isinstance(obj1, (Series, Categorical)):
            obj1 = Series(obj1)
        if not isinstance(obj2, (Series, Categorical)):
            obj2 = Series(obj2)

    if is_scalar(exclude):
        exclude = [exclude]

    if obj1 is obj2:
        if isinstance(obj1, Series):
            obj1 = obj2 = obj1[~obj1.isin(exclude)].reset_index(drop=True)
        else:
            obj1.remove_categories(
                [exc for exc in exclude if exc in obj1.categories],
                inplace=True
            )
    else:
        if isinstance(obj1, Series):
            obj1 = obj1[~obj1.isin(exclude)].reset_index(drop=True)
        else:
            obj1.remove_categories(
                [exc for exc in exclude if exc in obj1.categories],
                inplace=True
            )
        if isinstance(obj2, Series):
            obj2 = obj2[~obj2.isin(exclude)].reset_index(drop=True)
        else:
            obj2.remove_categories(
                [exc for exc in exclude if exc in obj2.categories],
                inplace=True
            )

    if NA not in exclude:
        if obj1 is obj2:
            if not is_categorical_dtype(obj1):
                obj1 = obj2 = obj1.fillna('<NA>')
        else:
            if not is_categorical_dtype(obj1):
                obj1 = obj1.fillna('<NA>')
            if not is_categorical_dtype(obj2):
                obj2 = obj2.fillna('<NA>')

    kwargs = {'dropna': False}
    if dn1:
        kwargs['rownames'] = [dn1]
    if dn2:
        kwargs['colnames'] = [dn2]
    tab = pandas.crosstab(obj1, obj2, **kwargs)
    if obj1 is obj2:
        tab = DataFrame(dict(count=numpy.diag(tab)), index=tab.columns).T
    return tab

@register_func(None, context=Context.EVAL)
def round(
        number: NumericOrIter,
        ndigits: int = 0
) -> NumericOrIter:
    """Rounding a number"""
    number = objectize(number)
    if is_series_like(number):
        return number.round(ndigits)
    return builtins.round(number, ndigits)

@register_func(None, context=Context.EVAL)
def sqrt(x: Any) -> bool:
    """Get the square root of a number"""
    x = objectize(x)
    return numpy.sqrt(x)

@register_func(None, context=Context.EVAL)
def sin(x: Any) -> NumericOrIter:
    """Get the sin of a number"""
    x = objectize(x)
    return numpy.sin(x)

@register_func(None, context=Context.EVAL)
def cos(x: Any) -> NumericOrIter:
    """Get the cos of a number"""
    x = objectize(x)
    return numpy.cos(x)

@register_func(None, context=Context.EVAL)
def droplevels(x: Categorical) -> Categorical:
    """drop unused levels from a factor

    Args:
        x: The categorical data

    Returns:
        The categorical data with unused categories dropped.
    """
    return categorize(x).remove_unused_categories()

# ---------------------------------
# Plain functions
# ---------------------------------

def factor(
        x: Iterable[Any],
        levels: Optional[Iterable[Any]] = None,
        exclude: Any = NA,
        ordered: bool = False
) -> Categorical:
    """encode a vector as a factor (the terms ‘category’ and ‘enumerated type’
    are also used for factors).

    If argument ordered is TRUE, the factor levels are assumed to be ordered

    Args:
        x: a vector of data
        levels: an optional vector of the unique values (as character strings)
            that x might have taken.
        exclude: a vector of values to be excluded when forming the set of
            levels. This may be factor with the same level set as x or
            should be a character
        ordered: logical flag to determine if the levels should be regarded
            as ordered (in the order given).
    """
    if is_categorical_dtype(x):
        x = x.to_numpy()
    ret = Categorical(
        objectize(x),
        categories=levels,
        ordered=ordered
    )
    if is_scalar(exclude):
        exclude = [exclude]

    return ret.remove_categories(exclude)

def rep(
        x: Any,
        times: Union[int, Iterable[int]] = 1,
        length: Optional[int] = None,
        each: int = 1
) -> Iterable[Any]:
    """replicates the values in x

    Args:
        x: a vector or scaler
        times: number of times to repeat each element if of length len(x),
            or to repeat the whole vector if of length 1
        length: non-negative integer. The desired length of the output vector
        each: non-negative integer. Each element of x is repeated each times.

    Returns:
        A list of repeated elements in x.
    """
    if is_scalar(x):
        x = [x]
    if is_iterable(times):
        if len(times) != len(x):
            raise ValueError(
                "Invalid times argument, expect length "
                f"{len(times)}, got {len(x)}"
            )
        if each != 1:
            raise ValueError(
                "Unexpected each argument when times is an iterable."
            )

    if is_int(times):
        x = [elem for elem in x for _ in range(each)] * int(times)
    else:
        x = [elem for n, elem in zip(times, x) for _ in range(n)]
    if length is None:
        return x
    repeats = length // len(x) + 1
    x = x * repeats
    return x[:length]

def context(data: DataFrameType) -> Any:
    """Evaluate verbs, functions in the
    possibly modifying (a copy of) the original data.

    It mimic the `with` function in R, but you have to write it in a python way,
    which is using the `with` statement. And you have to use it with `as`, since
    we need the value returned by `__enter__`.

    Args:
        data: The data
        func: A function that is registered by
            `pipda.register_verb` or `pipda.register_func`.
        *args: Arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        The original or modified data
    """
    return ContextWithData(data)
