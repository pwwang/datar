import datetime
import functools
from typing import Any, Iterable, Iterator, List, Optional, Union
import numpy
from numpy.core.numeric import NaN
from numpy.lib.function_base import vectorize
from pandas.core.dtypes.dtypes import CategoricalDtype
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from pipda import register_common, Context

from .middlewares import Collection, UnaryNeg

DateType = Union[int, str, datetime.date]

def register_vectorized(func):
    """Vectorize the common functions

    Note that only the first argument is vectorized.

    Args:
        func: The function to be vectorized
    """


    @register_common
    @functools.wraps(func)
    def wrapper(x, *args, **kwargs):
        partial_func = lambda y: func(y, *args, **kwargs)
        vec_func = vectorize(partial_func)
        return vec_func(x)
    return wrapper

@register_common(context=Context.NAME)
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

@register_common
def is_categorical(x: Series) -> bool:
    return isinstance(x.dtype, CategoricalDtype)

@register_common
def as_categorical(x: Series) -> Series:
    return x.astype('category')

@register_vectorized
def as_character(x: Any) -> str:
    return str(x)


@register_common
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
