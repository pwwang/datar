"""Function from R-base that can be used as verbs"""
from typing import Any, Iterable, List, Optional, Tuple, Union

import numpy
from pandas.core.frame import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from pipda import register_verb

from ..core.utils import objectize
from ..core.types import IntType, is_scalar, DataFrameType
from ..core.contexts import Context

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def colnames(
        df: DataFrameType,
        names: Optional[Iterable[str]] = None,
        copy: bool = False
) -> Union[List[Any], DataFrameType]:
    """Get or set the column names of a dataframe

    Args:
        df: The dataframe
        names: The names to set as column names for the dataframe.
        copy: Whether return a copy of dataframe with new columns

    Returns:
        A list of column names if names is None, otherwise return the dataframe
        with new column names.
        if the input dataframe is grouped, the structure is kept.
    """
    if names is None:
        return objectize(df).columns.tolist()

    grouper = getattr(df, 'grouper', None)
    ret = objectize(df)
    if copy:
        ret = ret.copy()
    ret.columns = names
    if grouper is not None:
        return ret.groupby(grouper, dropna=False)
    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def rownames(
        df: DataFrameType,
        names: Optional[Iterable[str]] = None,
        copy: bool = False
) -> Union[List[Any], DataFrameType]:
    """Get or set the row names of a dataframe

    Args:
        df: The dataframe
        names: The names to set as row names for the dataframe.
        copy: Whether return a copy of dataframe with new row names

    Returns:
        A list of row names if names is None, otherwise return the dataframe
        with new row names.
        if the input dataframe is grouped, the structure is kept.
    """
    if names is None:
        return objectize(df).index.tolist()

    grouper = getattr(df, 'grouper', None)
    ret = objectize(df)
    if copy:
        ret = ret.copy()
    ret.index = names
    if grouper is not None:
        return ret.groupby(grouper, dropna=False)
    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def dim(x: DataFrameType) -> Tuple[int]:
    """Retrieve the dimension of a dataframe.

    Args:
        x: a dataframe

    Returns:
        The shape of the dataframe.
    """
    return objectize(x).shape

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def nrow(_data: DataFrameType) -> int:
    """Get the number of rows in a dataframe

    Args:
        _data: The dataframe

    Returns:
        The number of rows in _data
    """
    return dim(_data)[0]

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def ncol(_data: DataFrameType):
    """Get the number of columns in a dataframe

    Args:
        _data: The dataframe

    Returns:
        The number of columns in _data
    """
    return dim(_data)[1]

@register_verb(context=Context.EVAL)
def diag(
        x: Any = 1,
        nrow: Optional[IntType] = None, # pylint: disable=redefined-outer-name
        ncol: Optional[IntType] = None  # pylint: disable=redefined-outer-name
) -> DataFrame:
    """Extract, construct a diagonal dataframe or replace the diagnal of
    a dataframe.

    When used with DataFrameGroupBy data, groups are ignored

    Args:
        x: a matrix, vector or scalar
        nrow, ncol: optional dimensions for the result when x is not a matrix.
            if nrow is an iterable, it will replace the diagnal of the input
            dataframe.

    Returns:
        If x is a matrix then diag(x) returns the diagonal of x.
        In all other cases the value is a diagonal matrix with nrow rows and
        ncol columns (if ncol is not given the matrix is square).
        Here nrow is taken from the argument if specified, otherwise
        inferred from x
    """
    if nrow is None and isinstance(x, int):
        nrow = x
        x = 1
    if ncol is None:
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

@register_verb((DataFrame, DataFrameGroupBy))
def _(
        x: DataFrameType,
        nrow: Any = None, # pylint: disable=redefined-outer-name
        ncol: Optional[IntType] = None  # pylint: disable=redefined-outer-name
) -> Union[DataFrame, numpy.ndarray]:
    """Diag when x is a dataframe"""
    if nrow is not None and ncol is not None:
        raise ValueError("Extra arguments received for diag.")

    x = x.copy()
    if nrow is not None:
        numpy.fill_diagonal(x, nrow)

    return x


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
