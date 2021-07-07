"""Function from R-base that can be used as verbs"""
from typing import Any, Iterable, List, Mapping, Tuple, Union, Sequence

import numpy
from pandas import DataFrame, Series, Categorical
from pipda import register_verb

from ..core.types import IntType, is_scalar
from ..core.contexts import Context
from ..core.utils import Array

# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument


@register_verb(DataFrame, context=Context.EVAL)
def colnames(
    df: DataFrame, new: Sequence[str] = None, _nested: bool = True
) -> Union[List[Any], DataFrame]:
    """Get or set the column names of a dataframe

    Args:
        df: The dataframe
        new: The new names to set as column names for the dataframe.

    Returns:
        A list of column names if names is None, otherwise return the dataframe
        with new column names.
        if the input dataframe is grouped, the structure is kept.
    """
    from ..stats.verbs import set_names

    if not _nested:
        if new is not None:
            return set_names(df, new)
        return df.columns.tolist()

    if new is not None:
        namei = 0
        newnames = []
        last_parts0 = None
        for colname in df.columns:
            parts = str(colname).split("$", 1)
            if not newnames:
                if len(parts) < 2:
                    newnames.append(new[namei])
                else:
                    last_parts0 = parts[0]
                    newnames.append(f"{new[namei]}${parts[1]}")
            elif len(parts) < 2:
                namei += 1
                newnames.append(new[namei])
            elif last_parts0 and colname.startswith(f"{last_parts0}$"):
                newnames.append(f"{new[namei]}${parts[1]}")
            else:
                namei += 1
                newnames.append(f"{new[namei]}${parts[1]}")
                last_parts0 = parts[0]
        return set_names(df, newnames)

    cols = [
        col.split("$", 1)[0] if isinstance(col, str) else col
        for col in df.columns
    ]
    out = []
    for col in cols:
        if col not in out:
            out.append(col)
    return out


@register_verb(DataFrame, context=Context.EVAL)
def rownames(
    df: DataFrame, new: Sequence[str] = None
) -> Union[List[Any], DataFrame]:
    """Get or set the row names of a dataframe

    Args:
        df: The dataframe
        new: The new names to set as row names for the dataframe.
        copy: Whether return a copy of dataframe with new row names

    Returns:
        A list of row names if names is None, otherwise return the dataframe
        with new row names.
        if the input dataframe is grouped, the structure is kept.
    """
    if new is not None:
        df = df.copy()
        df.index = new
        return df

    return df.index.tolist()


@register_verb(DataFrame, context=Context.EVAL)
def dim(x: DataFrame, _nested: bool = True) -> Tuple[int]:
    """Retrieve the dimension of a dataframe.

    Args:
        x: a dataframe
        _nested: When there is _nesteded df, count as 1.

    Returns:
        The shape of the dataframe.
    """
    return (nrow(x), ncol(x, _nested))


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
def ncol(_data: DataFrame, _nested: bool = True):
    """Get the number of columns in a dataframe

    Args:
        _data: The dataframe
        _nested: When there is _nesteded df, count as 1.

    Returns:
        The number of columns in _data
    """
    if not _nested:
        return _data.shape[1]
    cols = set()
    for col in _data.columns:
        cols.add(col.split("$", 1)[0] if isinstance(col, str) else col)
    return len(cols)


@register_verb(context=Context.EVAL)
def diag(
    x: Any = 1,
    nrow: IntType = None,  # pylint: disable=redefined-outer-name
    ncol: IntType = None,  # pylint: disable=redefined-outer-name
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

    x = Array(x)
    ret = DataFrame(numpy.diag(x), dtype=x.dtype)
    return ret.iloc[:nrow, :ncol]


@diag.register(DataFrame)
def _(
    x: DataFrame,
    nrow: Any = None,  # pylint: disable=redefined-outer-name
    ncol: IntType = None,  # pylint: disable=redefined-outer-name
) -> Union[DataFrame, numpy.ndarray]:
    """Diag when x is a dataframe"""
    if nrow is not None and ncol is not None:
        raise ValueError("Extra arguments received for diag.")

    x = x.copy()
    if nrow is not None:
        numpy.fill_diagonal(x.values, nrow)
        return x
    return numpy.diag(x)


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
def names(
    x: DataFrame, new: Sequence[str] = None, _nested: bool = True
) -> Union[List[str], DataFrame]:
    """Get the column names of a dataframe"""
    return colnames(x, new, _nested)

@names.register(dict)
def _(
        x: Mapping[str, Any],
        new: Iterable[str] = None,
        _nested: bool = True
) -> Union[List[str], Mapping[str, Any]]:
    """Get the keys of a dict
    dict is like a list in R, mimic `names(<list>)` in R.
    """
    if new is None:
        return list(x)
    return dict(zip(new, x.values()))

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


@register_verb((list, tuple, numpy.ndarray, Series, Categorical))
def duplicated(  # pylint: disable=invalid-name
    x: Iterable[Any],
    incomparables: Sequence[Any] = None,
    from_last: bool = False,
) -> numpy.ndarray:
    """Determine Duplicate Elements

    Args:
        x: The iterable to detect duplicates
            Currently, elements in `x` must be hashable.
        from_last: Whether start to detect from the last element

    Returns:
        A bool array with the same length as `x`
    """
    dups = set()
    out = []
    out_append = out.append
    if incomparables is None:
        incomparables = []

    if from_last:
        x = reversed(x)
    for elem in x:
        if elem in incomparables:
            out_append(False)
        elif elem in dups:
            out_append(True)
        else:
            dups.add(elem)
            out_append(False)
    if from_last:
        out = list(reversed(out))
    return Array(out, dtype=bool)


@duplicated.register(DataFrame)
def _(  # pylint: disable=invalid-name,unused-argument
    x: DataFrame,
    incomparables: Iterable[Any] = None,
    from_last: bool = False,
) -> numpy.ndarray:
    """Check if rows in a data frame are duplicated

    `incomparables` not working here
    """
    keep = "first" if not from_last else "last"
    return x.duplicated(keep=keep).values
