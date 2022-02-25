"""Function from R-base that can be used as verbs"""
from typing import Any, Iterable, Sequence, Tuple, Union

import numpy as np
import pandas as pd
from pandas._typing import AnyArrayLike
from pandas.api.types import is_scalar
from pandas import Categorical, DataFrame, Series, Index
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.utils import (
    arg_match,
    regcall,
    ensure_nparray,
)


@register_verb(DataFrame, context=Context.EVAL)
def colnames(
    df: DataFrame,
    new: Sequence[str] = None,
    nested: bool = True,
) -> Union[Sequence[str], DataFrame]:
    """Get or set the column names of a dataframe

    Args:
        df: The dataframe
        new: The new names to set as column names for the dataframe.
            It's like `R` operation: `colnames(df) <- c(...)`
        nested: If `df` has no nested dataframes, this option has no effect.
            Whether treat the nested dataframes as a whole

    Examples:
        >>> df = tibble(x=1, y=tibble(a=2, b=4))
        >>> colnames(df)  # ['x', 'y']
        >>> colnames(df, nested=False)  # ['x', 'y$a', 'y$b']
        >>> colnames(df, ['m', 'n'])  # df with new names ['m', 'n$a', 'n$b']
        >>> colnames(df, ['m', 'n'], nested=False)
        >>> # df with new names: ['m', 'n', 'y$b']

    Returns:
        A list of column names if new is None, otherwise return the dataframe
        with new column names.
        if the input dataframe is grouped, the structure is kept.
    """
    has_nest = any("$" in str(col) for col in df.columns)
    if not has_nest or not nested:
        if new is None:
            return df.columns.values
        df = df.copy()
        df.columns = new
        return df

    # x, y, y -> x, y
    names = Index([col.split("$", 1)[0] for col in df.columns]).unique()
    mappings = dict(zip(names, new))
    newnames = []
    for col in df.columns:
        if col in mappings:
            newnames.append(mappings[col])
        else:
            name, subcol = col.split("$", 1)
            newnames.append(f"{mappings[name]}${subcol}")

    df = df.copy()
    df.columns = newnames
    return df


names = colnames


@register_verb(DataFrame, context=Context.EVAL)
def rownames(
    df: DataFrame, new: Sequence[str] = None
) -> Union[Sequence[Any], DataFrame]:
    """Get or set the row names of a dataframe

    Args:
        df: The dataframe
        new: The new names to set as row names for the dataframe.

    Returns:
        A list of row names if names is None, otherwise return the dataframe
        with new row names.
        if the input dataframe is grouped, the structure is kept.
    """
    if new is not None:
        df = df.copy()
        df.index = new
        return df

    return df.index.values


@register_verb(DataFrame, context=Context.EVAL)
def dim(x: DataFrame, nested: bool = True) -> Tuple[int]:
    """Retrieve the dimension of a dataframe.

    Args:
        x: a dataframe
        nested: When there is _nesteded df, count as 1.

    Returns:
        The shape of the dataframe.
    """
    return (regcall(nrow, x), regcall(ncol, x, nested))


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
def ncol(_data: DataFrame, nested: bool = True):
    """Get the number of columns in a dataframe

    Args:
        _data: The dataframe
        nested: When there is _nesteded df, count as 1.

    Returns:
        The number of columns in _data
    """
    if not nested:
        return _data.shape[1]

    return len(regcall(colnames, _data, nested=nested))


@register_verb(context=Context.EVAL)
def diag(
    x: Any = 1,
    nrow: int = None,
    ncol: int = None,
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

    x = np.array(x)
    ret = DataFrame(np.diag(x), dtype=x.dtype)
    return ret.iloc[:nrow, :ncol]


@diag.register(DataFrame)
def _(
    x: DataFrame,
    nrow: Any = None,
    ncol: int = None,
) -> Union[DataFrame, np.ndarray]:
    """Diag when x is a dataframe"""
    if nrow is not None and ncol is not None:
        raise ValueError("Extra arguments received for diag.")

    x = x.copy()
    if nrow is not None:
        np.fill_diagonal(x.values, nrow)
        return x
    return np.diag(x)


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


@register_verb(context=Context.EVAL)
def setdiff(x: Any, y: Any) -> np.ndarray:
    """Diff of two iterables"""
    return np.array([elem for elem in x if elem not in frozenset(y)])


@register_verb(context=Context.EVAL)
def intersect(x: Any, y: Any) -> np.ndarray:
    """Intersect of two iterables"""
    # order not kept
    # return np.intersect1d(x, y)
    x = pd.unique(ensure_nparray(x))
    return np.array([elem for elem in x if elem in frozenset(y)])


@register_verb(context=Context.EVAL)
def union(x: Any, y: Any) -> np.ndarray:
    """Union of two iterables"""
    # order not kept
    # return np.union1d(x, y)
    out = np.concatenate([ensure_nparray(x), ensure_nparray(y)])
    return pd.unique(out)


@register_verb(context=Context.EVAL)
def unique(x: Any) -> np.ndarray:
    """Union of two iterables"""
    # order not kept
    # return np.unique(x)
    return pd.unique(x)


@register_verb(context=Context.EVAL)
def setequal(x: Any, y: Any, equal_na: bool = True) -> bool:
    """Check set equality for two iterables (order doesn't matter)"""
    return np.array_equal(
        ensure_nparray(x),
        ensure_nparray(y),
        equal_nan=equal_na,
    )


@register_verb(
    (Sequence, np.ndarray, Series, Categorical),
    context=Context.EVAL,
)
def append(x: Any, values: Any, after: int = -1) -> np.ndarray:
    """Add elements to a vector.

    Args:
        x: the vector the values are to be appended to.
        values: to be included in the modified vector.
        after: a subscript, after which the values are to be appended.

    Returns:
        A vector containing the values in ‘x’ with the elements of
        ‘values’ appended after the specified element of ‘x’.
    """
    x = ensure_nparray(x)
    if after < 0:
        after += len(x) + 1
    else:
        after += 1
    return np.insert(x, after, values)


@register_verb((list, tuple, np.ndarray, Series, Categorical))
def duplicated(
    x: Iterable[Any],
    incomparables: Sequence[Any] = None,
    from_last: bool = False,
) -> np.ndarray:
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
    return np.array(out, dtype=bool)


@duplicated.register(DataFrame)
def _(
    x: DataFrame,
    incomparables: Iterable[Any] = None,
    from_last: bool = False,
) -> np.ndarray:
    """Check if rows in a data frame are duplicated

    `incomparables` not working here
    """
    keep = "first" if not from_last else "last"
    return x.duplicated(keep=keep).values


@register_verb(DataFrame)
def max_col(
    df: DataFrame, ties_method: str = "random"
) -> Iterable[int]:
    """Find the maximum position for each row of a matrix

    Args:
        df: The data frame
        ties_method: how ties are handled
            - "random": use a random index
            - "first" use the first index
            - "last" use the last index

    Returns:
        The indices of max values for each row
    """
    ties_method = arg_match(
        ties_method, "ties_method", ["random", "first", "last"]
    )

    def which_max_with_ties(ser: Series):
        """Find index with max if ties happen"""
        indices = np.flatnonzero(ser == max(ser))
        if len(indices) == 1 or ties_method == "first":
            return indices[0]
        if ties_method == "random":
            return np.random.choice(indices)
        return indices[-1]

    return df.apply(which_max_with_ties, axis=1).to_np()


@register_verb(DataFrame)
def complete_cases(_data: DataFrame) -> np.ndarray:
    """Return a logical vector indicating values of rows are complete.

    Args:
        _data: The dataframe

    Returns:
        A logical vector specifying which observations/rows have no
        missing values across the entire sequence.
    """
    return _data.apply(lambda row: row.notna().all(), axis=1).values


@register_verb(DataFrame)
def proportions(
    x: DataFrame, margin: Union[int, tuple, list] = None
) -> DataFrame:
    """Returns conditional proportions given `margins` (alias: prop_table)

    Args:
        x: A numeric table
        margin: If x is a dataframe, 1 for rows, 2 for columns, and 3 or [1,2]
            for both rows and columns (turns x to a unit matrix with all
            elements equal to 1)

    Returns:
        The x but with given margin replaced with proportion
    """
    from ..dplyr import mutate, rename_with, across, everything
    from . import sum_

    if margin is None:
        sumall = x.to_np().sum()
        return x.applymap(lambda elem: elem / sumall, na_action="ignore")

    if margin == 1:
        index = x.index
        out = t(
            proportions(
                rename_with(
                    t(x, __calling_env=CallingEnvs.REGULAR),
                    str,
                    __calling_env=CallingEnvs.REGULAR,
                ),
                2,
                __calling_env=CallingEnvs.REGULAR,
            ),
            __calling_env=CallingEnvs.REGULAR,
        )
        out.index = index
        return out

    if margin == 2:
        return mutate(
            x,
            across(
                everything(__calling_env=CallingEnvs.PIPING),
                lambda col: col / sum_(col, __calling_env=CallingEnvs.REGULAR),
                __calling_env=CallingEnvs.PIPING,
            ),
            __calling_env=CallingEnvs.REGULAR,
        )

    return x.applymap(lambda elem: 1, na_action="ignore")


prop_table = proportions


@proportions.register((list, tuple, np.array, Series))
def _(x, margin: Union[int, tuple, list] = None) -> AnyArrayLike:
    """proportions for vectors"""
    x = ensure_nparray(x)
    return x / np.sum(x)
