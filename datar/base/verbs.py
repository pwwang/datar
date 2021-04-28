"""Function from R-base that can be used as verbs"""
# TODO: add tests
from typing import Any, Iterable, List, Optional, Tuple, Union

import numpy
from pandas import DataFrame
from pipda import register_verb

from ..core.types import IntType, is_scalar
from ..core.contexts import Context

# pylint: disable=redefined-outer-name

@register_verb(DataFrame, context=Context.EVAL)
def colnames(
        df: DataFrame,
        names: Optional[Iterable[str]] = None
) -> Union[List[Any], DataFrame]:
    """Get or set the column names of a dataframe

    Args:
        df: The dataframe
        names: The names to set as column names for the dataframe.

    Returns:
        A list of column names if names is None, otherwise return the dataframe
        with new column names.
        if the input dataframe is grouped, the structure is kept.
    """
    from ..stats.verbs import setNames
    if names is not None:
        return setNames(df, names)

    return df.columns.tolist()

@register_verb(DataFrame, context=Context.EVAL)
def rownames(
        df: DataFrame,
        names: Optional[Iterable[str]] = None
) -> Union[List[Any], DataFrame]:
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
    if names is not None:
        df = df.copy()
        df.index = names
        return df

    return df.index.tolist()

@register_verb(DataFrame, context=Context.EVAL)
def dim(x: DataFrame, stack: bool = True) -> Tuple[int]:
    """Retrieve the dimension of a dataframe.

    Args:
        x: a dataframe
        stack: When there is stacked df, count as 1.

    Returns:
        The shape of the dataframe.
    """
    return (nrow(x), ncol(x, stack))

@register_verb(DataFrame)
def nrow(_data: DataFrame) -> int:
    """Get the number of rows in a dataframe

    Args:
        _data: The dataframe

    Returns:
        The number of rows in _data
    """
    return _data.shape[0]

@register_verb(DataFrame)
def ncol(_data: DataFrame, stack: bool = True):
    """Get the number of columns in a dataframe

    Args:
        _data: The dataframe
        stack: When there is stacked df, count as 1.

    Returns:
        The number of columns in _data
    """
    if not stack:
        return _data.shape[1]
    cols = set()
    for col in _data.columns:
        cols.add(col.split('$', 1)[0] if isinstance(col, str) else col)
    return len(cols)

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

# @diag.register(DataFrame)
# def _(
#         x: DataFrame,
#         nrow: Any = None, # pylint: disable=redefined-outer-name
#         ncol: Optional[IntType] = None  # pylint: disable=redefined-outer-name
# ) -> Union[DataFrame, numpy.ndarray]:
#     """Diag when x is a dataframe"""
#     if nrow is not None and ncol is not None:
#         raise ValueError("Extra arguments received for diag.")

#     x = x.copy()
#     if nrow is not None:
#         numpy.fill_diagonal(x, nrow)

#     return x


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

@register_verb(DataFrame)
def names(x: DataFrame) -> List[str]:
    """Get the column names of a dataframe"""
    return x.columns.tolist()

@register_verb(context=Context.EVAL)
def setdiff(x: Any, y: Any) -> List[Any]:
    """Diff of two iterables"""
    if is_scalar(x):
        x = [x]
    if is_scalar(y):
        y = [y]
    return [elem for elem in x if elem not in y]


@register_verb(context=Context.EVAL)
def intersect(x: Any, y: Any) -> List[Any]:
    """Intersect of two iterables"""
    if is_scalar(x):
        x = [x]
    if is_scalar(y):
        y = [y]
    return [elem for elem in x if elem in y]


@register_verb(context=Context.EVAL)
def union(x: Any, y: Any) -> List[Any]:
    """Union of two iterables"""
    if is_scalar(x):
        x = [x]
    if is_scalar(y):
        y = [y]
    # pylint: disable=arguments-out-of-order
    return list(x) + setdiff(y, x)

@register_verb(context=Context.EVAL)
def setequal(x: Any, y: Any) -> List[Any]:
    """Check set equality for two iterables (order doesn't matter)"""
    if is_scalar(x):
        x = [x]
    if is_scalar(y):
        y = [y]
    x = sorted(x)
    y = sorted(y)
    return x == y
