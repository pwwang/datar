"""Date time functions"""

import datetime
import functools
from typing import Any, List, Union

import numpy as np
import pandas as pd
from pandas import Series
from pandas.api.types import is_scalar, is_integer

from ..core.utils import transform_func, transform_func_decor


@functools.singledispatch
def _as_date_dummy(
    x: Any,
    format: str = None,
    try_formats: List[str] = None,
    optional: bool = False,
    tz: Union[int, datetime.timedelta] = 0,
    origin: Any = None,
) -> datetime.date:
    """Convert a dummy object to date"""
    raise ValueError(f"Unable to convert to date with type: {type(x)!r}")


@_as_date_dummy.register(datetime.date)
def _(
    x: datetime.date,
    format: str = None,
    try_formats: List[str] = None,
    optional: bool = False,
    tz: Union[int, datetime.timedelta] = 0,
    origin: Any = None,
) -> datetime.date:
    if is_scalar(tz) and is_integer(tz):
        tz = datetime.timedelta(hours=int(tz))

    return x + tz


@_as_date_dummy.register(datetime.datetime)
def _(
    x: datetime.datetime,
    format: str = None,
    try_formats: List[str] = None,
    optional: bool = False,
    tz: Union[int, datetime.timedelta] = 0,
    origin: Any = None,
):
    if is_scalar(tz) and is_integer(tz):
        tz = datetime.timedelta(hours=int(tz))

    return (x + tz).date()


@_as_date_dummy.register(str)
def _(
    x: str,
    format: str = None,
    try_formats: List[str] = None,
    optional: bool = False,
    tz: Union[int, datetime.timedelta] = 0,
    origin: Any = None,
) -> datetime.date:
    if is_scalar(tz) and is_integer(tz):
        tz = datetime.timedelta(hours=int(tz))

    try_formats = try_formats or [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
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
        return np.nan

    raise ValueError(
        "character string is not in a standard unambiguous format"
    )


@_as_date_dummy.register(Series)
def _(
    x: Series,
    format: str = None,
    try_formats: List[str] = None,
    optional: bool = False,
    tz: Union[int, datetime.timedelta] = 0,
    origin: Any = None,
) -> datetime.date:
    return _as_date_dummy(
        x.values[0],
        format=format,
        try_formats=try_formats,
        optional=optional,
        tz=tz,
        origin=origin,
    )


@_as_date_dummy.register(int)
@_as_date_dummy.register(np.integer)
def _(
    x: int,
    format: str = None,
    try_formats: List[str] = None,
    optional: bool = False,
    tz: Union[int, datetime.timedelta] = 0,
    origin: Any = None,
) -> datetime.date:
    if isinstance(tz, (int, np.integer)):
        tz = datetime.timedelta(hours=int(tz))

    if isinstance(origin, str):
        origin = _as_date_dummy(origin)

    dt = origin + datetime.timedelta(days=int(x)) + tz

    if isinstance(dt, datetime.datetime):
        return dt.date()
    return dt


@transform_func_decor(vectorized=False)
def as_date(
    x,
    format: str = None,
    try_formats: List[str] = None,
    optional: bool = False,
    tz: Union[int, datetime.timedelta] = 0,
    origin: Any = None,
) -> Series:
    """Convert an object to a datetime.date object

    See: https://rdrr.io/r/base/as.Date.html

    Args:
        x: Object that can be converted into a datetime.date object
        format:  If not specified, it will try try_formats one by one on
            the first non-np.nan element, and give an error if none works.
            Otherwise, the processing is via strptime
        try_formats: vector of format strings to try if format is not specified.
            Default formats to try:
            "%Y-%m-%d"
            "%Y/%m/%d"
            "%Y-%m-%d %H:%M:%S"
            "%Y/%m/%d %H:%M:%S"
        optional: indicating to return np.nan (instead of signalling an error)
            if the format guessing does not succeed.
        origin: a datetime.date/datetime object, or something which can be
            coerced by as_date(origin, ...) to such an object.
        tz: a time zone offset or a datetime.timedelta object.
            Note that time zone name is not supported yet.

    Returns:
        The datetime.date object
    """
    return pd.to_datetime(
        _as_date_dummy(
            x,
            format=format,
            try_formats=try_formats,
            optional=optional,
            origin=origin,
            tz=tz,
        )
    )


as_pd_date = transform_func(
    "as_pd_date",
    doc="""Alias of pandas.to_datetime(), but registered as a function
    so that it can be used in verbs.

    See https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html

    Args:
        x: The argument to be converted to datetime
        *args: and
        **kwargs: Other arguments passing to `pandas.to_datetime()`

    Returns:
        Converted datetime
    """,
    transform=pd.to_datetime,
)
