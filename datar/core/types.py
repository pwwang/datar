"""Types for convenience"""
from typing import Any, Iterable, Union

# pylint: disable=unused-import
import numpy
from numpy.core.numerictypes import ScalarType
from pandas.core.frame import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy, SeriesGroupBy
from pandas.core.series import Series

# used for type annotations
NumericType = Union[int, float, complex, numpy.number]
IntType = Union[int, numpy.integer]
FloatType = Union[float, numpy.float]
DataFrameType = Union[DataFrame, DataFrameGroupBy]
SeriesType = Union[Series, SeriesGroupBy]
SeriesLikeType = Union[Series, SeriesGroupBy, numpy.ndarray]
StringOrIter = Union[str, Iterable[str]]
IntOrIter = Union[IntType, Iterable[IntType]]
DoubleOrIter = Union[numpy.double, Iterable[numpy.double]]
BoolOrIter = Union[bool, Iterable[bool]]
FloatOrIter = Union[FloatType, Iterable[FloatType]]
NumericOrIter = Union[NumericType, Iterable[NumericType]]

# used for type checks
def is_int(x: Any) -> bool:
    """Check if a value is an integer"""
    return isinstance(x, (int, numpy.integer))

def is_series_like(x: Any) -> bool:
    """Check if a value is series like, which can do something in common.
    For example: .astype, .cumsum, etc
    """
    return isinstance(x, (Series, SeriesGroupBy, numpy.ndarray))

def is_scalar(x: Any) -> bool:
    """Check if a value is scalar.

    None will be counted as scalar
    """
    if x is None:
        return True
    return numpy.isscalar(x)

def is_iterable(x: Any) -> bool:
    """Check if a value is iterable, which is not a scalar"""
    return not is_scalar(x)

