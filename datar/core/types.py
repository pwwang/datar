"""Types for convenience"""
from typing import Any, Iterable, Union, List, Type, Tuple

# pylint: disable=unused-import
import numpy

from pandas import Categorical, Series, isnull as is_null
from pandas.api.types import is_scalar as is_scalar_, is_list_like
from pandas.core.dtypes.common import is_categorical_dtype as is_categorical

# used for type annotations
Dtype = Union[
    str, numpy.dtype, Type[Union[str, float, int, complex, bool, object]]
]
NumericType = Union[int, float, complex, numpy.number]
IntType = Union[int, numpy.integer]
FloatType = Union[float, numpy.float_]
ComplexType = Union[complex, numpy.complex_]
StringType = Union[str, numpy.str_]
ArrayLikeType = Union[Series, List, Tuple, numpy.ndarray]
CategoricalLikeType = Union[Series, Categorical]
StringOrIter = Union[StringType, Iterable[StringType]]
IntOrIter = Union[IntType, Iterable[IntType]]
DoubleOrIter = Union[numpy.double, Iterable[numpy.double]]
BoolOrIter = Union[bool, Iterable[bool]]
FloatOrIter = Union[FloatType, Iterable[FloatType]]
NumericOrIter = Union[NumericType, Iterable[NumericType]]
ComplexOrIter = Union[ComplexType, Iterable[ComplexType]]
TypeOrIter = Union[Type, Iterable[Type]]
DtypeOrIter = Union[Dtype, Iterable[Dtype]]
NoneType = type(None)
# nan is float, a better NA annotation?
NAType = Union[NoneType, type(numpy.nan)]  # type: ignore
NAOrIter = Union[NAType, Iterable[NAType]] # type: ignore

# used for type checks
def is_scalar_int(x: Any) -> bool:
    """Check if a value is an integer"""
    return isinstance(x, (int, numpy.integer))


def is_scalar(x: Any) -> bool:
    """Check if a value is scalar.

    None will be counted as scalar
    """
    ret = is_scalar_(x)
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
