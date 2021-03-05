"""Some functions ported from R-base"""
import builtins
import datetime
import functools
import math
from string import ascii_letters as letters

from pandas.core.dtypes.common import is_categorical_dtype

from plyrda.middlewares import Collection, ContextWithData
from typing import Any, Iterable, List, Optional, Type, Union

import numpy
import pandas
from pandas import Series, Categorical, DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy, SeriesGroupBy
from pipda import register_func, register_verb, Context

from .utils import IterableLiterals, NumericType, objectize, register_grouped

NA = numpy.nan
TRUE = True
FALSE = False
NULL = None

pi = math.pi
Inf = numpy.inf

@functools.singledispatch
def _as_date_dummy(
        x: Any,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
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
    if isinstance(tz, (int, numpy.integer)):
        tz = datetime.timedelta(hours=int(tz))

    return x + tz

@_as_date_dummy.register(datetime.datetime)
def _(
        x: datetime.datetime,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
        origin: Any = None
):
    if isinstance(tz, (int, numpy.integer)):
        tz = datetime.timedelta(hours=int(tz))

    return (x + tz).date()

@_as_date_dummy.register(str)
def _(
        x: str,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
        origin: Any = None
) -> datetime.date:
    if isinstance(tz, (int, numpy.integer)):
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
    else:
        if optional:
            return numpy.nan

        raise ValueError(
            "character string is not in a standard unambiguous format"
        )

@_as_date_dummy.register(Series)
def _(
        x: Series,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
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
        x: str,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
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


def as_date(
        x: Any,
        format: Optional[str] = None,
        try_formats: Optional[List[str]] = None,
        optional: bool = False,
        tz: Union[int, datetime.timedelta] = 0,
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
        x = Series([x]) if not isinstance(x, IterableLiterals) else Series(x)

    return objectize(x).transform(
        _as_date_dummy,
        format=format,
        try_formats=try_formats,
        optional=optional,
        tz=tz,
        origin=origin
    )

def _as_type(x: Any, type: Type) -> Any:
    """Convert x or elements of x to certain type"""
    x = objectize(x)
    if isinstance(x, (numpy.ndarray, Series)):
        return x.astype(type)
    if isinstance(x, IterableLiterals):
        return builtins.type(x)(map(type, x))

@register_func(None, context=Context.EVAL)
def as_character(x: Any) -> Union[str, Iterable[str]]:
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
def as_double(x: Any) -> Union[numpy.double, Iterable[numpy.double]]:
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
    return _as_type(x, numpy.int64)

as_int64 = as_integer

@register_func(None, context=Context.EVAL)
def as_logical(x: Any) -> Union[bool, Iterable[bool]]:
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

@register_func(None, context=Context.EVAL)
def ceiling(x: NumericType) -> int:
    """Get the ceiling integer of x

    Args:
        x: The number

    Returns:
        The ceiling integer of x
    """
    return numpy.vectorize(math.ceil)(x)

@register_verb((DataFrame, DataFrameGroupBy))
def colnames(df: Union[DataFrame, DataFrameGroupBy]) -> List[str]:
    """Get the column names of a dataframe

    Args:
        df: The dataframe

    Returns:
        A list of column names
    """
    return objectize(df).columns.tolist()

@register_grouped(context=Context.EVAL)
def cumsum(
        series: Iterable[NumericType],
        skipna: bool = False
) -> Iterable[float]:
    if not isinstance(series, Series):
        series = Series(series)
    return series.cumsum(skipna=skipna)

@register_grouped(context=Context.EVAL)
def cummin(
        series: Iterable[NumericType],
        skipna: bool = False
) -> Iterable[float]:
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummin(skipna=skipna)

@register_grouped(context=Context.EVAL)
def cummax(
        series: Iterable[NumericType],
        skipna: bool = False
) -> Iterable[float]:
    if not isinstance(series, Series):
        series = Series(series)
    return series.cummax(skipna=skipna)

@register_grouped(context=Context.EVAL)
def cumprod(
        series: Iterable[NumericType],
        skipna: bool = False
) -> Iterable[float]:
    if not isinstance(series, Series):
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
def table(
        obj: Any,
        *objs: Any,
        exclude: Any = NA,
        use_na: str = "always",
        dnn: Optional[Union[str, List[str]]] = None,
        # not supported, varname.argname not working with wrappers having
        # different signatures.
        # deparse_level: int = 1
) -> DataFrame:
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
    elif isinstance(dnn, IterableLiterals):
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

    if not isinstance(exclude, IterableLiterals) or isinstance(exclude, str):
        exclude = [exclude]

    if obj1 is obj2:
        if isinstance(obj1, Series):
            obj1 = obj2 = obj1[~obj1.isin(exclude)].reset_index(drop=True)
        else:
            obj1.remove_categories(exclude, inplace=True)
    else:
        if isinstance(obj1, Series):
            obj1 = obj1[~obj1.isin(exclude)].reset_index(drop=True)
        else:
            obj1.remove_categories(exclude, inplace=True)
        if isinstance(obj2, Series):
            obj2 = obj2[~obj2.isin(exclude)].reset_index(drop=True)
        else:
            obj2.remove_categories(exclude, inplace=True)

    if NA not in exclude:
        if obj1 is obj2:
            if not is_categorical_dtype(obj1):
                obj1 = obj2 = obj1.fillna('NA')
        else:
            if not is_categorical_dtype(obj1):
                obj1 = obj1.fillna('NA')
            if not is_categorical_dtype(obj2):
                obj2 = obj2.fillna('NA')

    kwargs = {'dropna': False}
    if dn1:
        kwargs['rownames'] = [dn1]
    if dn2:
        kwargs['colnames'] = [dn2]
    tab = pandas.crosstab(obj1, obj2, **kwargs)
    if obj1 is obj2:
        tab = DataFrame(dict(count=numpy.diag(tab)), index=tab.columns).T
    return tab

def context(
        data: Union[DataFrame, DataFrameGroupBy]
) -> Any:
    """Evaluate verbs, functions in the
    possibly modifying (a copy of) the original data.

    It mimic the `with` function in R.

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


@register_grouped(context=Context.EVAL)
def quantile(
        series: Iterable[Any],
        probs: Union[float, Iterable[float]] = (0.0, 0.25, 0.5, 0.75, 1.0),
        na_rm: bool = False
):
    return (
        numpy.nanquantile(series, probs) if na_rm
        else numpy.quantile(series, probs)
    )

@register_grouped(context=Context.EVAL)
def sample(
        x: Union[int, Iterable[Any]],
        size: Optional[int] = None,
        replace: bool = False,
        prob: Optional[Iterable[Union[int, float]]] = None
) -> Iterable[Any]:
    """https://rdrr.io/r/base/sample.html"""
    if isinstance(x, str):
        x = list(x)
    if size is None:
        size = len(x) if isinstance(x, IterableLiterals) else x
    return numpy.random.choice(x, int(size), replace=replace, p=prob)
