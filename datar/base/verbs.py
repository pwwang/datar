"""Function from R-base that can be used as verbs"""
import numpy
from typing import Any, List, Optional, Tuple, Union
from pandas.core.frame import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from pipda import register_verb, Context

from ..core.utils import objectize
from ..core.types import IntType, is_scalar, DataFrameType

@register_verb((DataFrame, DataFrameGroupBy))
def colnames(df: DataFrameType) -> List[Any]:
    """Get the column names of a dataframe

    Args:
        df: The dataframe

    Returns:
        A list of column names
    """
    return objectize(df).columns.tolist()

@register_verb((DataFrame, DataFrameGroupBy))
def rownames(df: DataFrameType) -> List[Any]:
    """Get the column names of a dataframe

    Args:
        df: The dataframe

    Returns:
        A list of column names
    """
    return objectize(df).index.tolist()

@register_verb((DataFrame, DataFrameGroupBy))
def dim(x: DataFrameType) -> Tuple[int]:
    """Retrieve the dimension of a dataframe.

    Args:
        x: a dataframe

    Returns:
        The shape of the dataframe.
    """
    return objectize(x).shape

@register_verb((DataFrame, DataFrameGroupBy))
def nrow(_data: Union[DataFrame, DataFrameGroupBy]) -> int:
    """Get the number of rows in a dataframe

    Args:
        _data: The dataframe

    Returns:
        The number of rows in _data
    """
    return dim(_data)[0]

@register_verb((DataFrame, DataFrameGroupBy))
def ncol(_data: Union[DataFrame, DataFrameGroupBy]):
    """Get the number of columns in a dataframe

    Args:
        _data: The dataframe

    Returns:
        The number of columns in _data
    """
    return dim(_data)[1]

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def diag(
        x: Any = 1,
        nrow: Optional[IntType] = None,
        ncol: Optional[IntType] = None
) -> Union[DataFrame, numpy.ndarray]:
    """Extract or construct a diagonal matrix.

    When used with DataFrameGroupBy data, groups are ignored

    Args:
        x: a matrix, vector or scalar
        nrow, ncol: optional dimensions for the result when x is not a matrix.

    Returns:
        If x is a matrix then diag(x) returns the diagonal of x.
        In all other cases the value is a diagonal matrix with nrow rows and
        ncol columns (if ncol is not given the matrix is square).
        Here nrow is taken from the argument if specified, otherwise
        inferred from x
    """
    if isinstance(x, (DataFrame, DataFrameGroupBy)):
        return numpy.diag(objectize(x))
    if nrow is None and isinstance(x, int):
        nrow = x
        x = 1
    if ncol == None:
        ncol = nrow
    if is_scalar(x):
        nmax = max(nrow, ncol)
        x = [x] * nmax
    elif nrow is not None:
        nmax = max(nrow, ncol)
        nmax = nmax // len(x)
        x = x * nmax

    series = numpy.array(x)
    ret = DataFrame(numpy.diag(series))
    return ret.iloc[:nrow, :ncol]

@register_verb(DataFrame)
def t(_data: DataFrame, copy: bool = False) -> DataFrame:
    """Get the transposed dataframe

    Args:
        _data: The dataframe
        copy: When copy the data in memory

    Returns:
        The transposed dataframe.
    """
    return _data.transpose(copy=copy)
