"""Types for convenience"""
from typing import Any, Iterable, Union, List

# pylint: disable=unused-import
import numpy

from pandas import Categorical, Series, isnull as is_null
from pandas.core.dtypes.common import is_categorical_dtype as is_categorical
from pandas._typing import Dtype

# used for type annotations
NumericType = Union[int, float, complex, numpy.number]
IntType = Union[int, numpy.integer]
FloatType = Union[float, numpy.float64]
SeriesLikeType = Union[Series, List, numpy.ndarray]
CategoricalLikeType = Union[Series, Categorical]
StringOrIter = Union[str, Iterable[str]]
IntOrIter = Union[IntType, Iterable[IntType]]
DoubleOrIter = Union[numpy.double, Iterable[numpy.double]]
BoolOrIter = Union[bool, Iterable[bool]]
FloatOrIter = Union[FloatType, Iterable[FloatType]]
NumericOrIter = Union[NumericType, Iterable[NumericType]]

NoneType = type(None)
# used for type checks
def is_scalar_int(x: Any) -> bool:
    """Check if a value is an integer"""
    return isinstance(x, (int, numpy.integer))

def is_series_like(x: Any) -> bool:
    """Check if a value is series like, which can do something in common.
    For example: .astype, .cumsum, etc
    """
    return isinstance(x, (Series, numpy.ndarray))

def is_scalar(x: Any) -> bool:
    """Check if a value is scalar.

    None will be counted as scalar
    """
    ret = numpy.isscalar(x)
    if ret:
        return ret
    try:
        iter(x)
    except TypeError:
        return True
    return False

def is_iterable(x: Any) -> bool:
    """Check if a value is iterable, which is not a scalar"""
    return not is_scalar(x)

def is_not_null(x: Any) -> BoolOrIter:
    """Invert of is_null (pandas.isnull)"""
    return ~is_null(x)
