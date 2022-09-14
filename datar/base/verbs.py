"""Function from R-base that can be used as verbs"""
import numpy as np
from pipda import register_verb

from ..core.backends import pandas as pd
from ..core.backends.pandas import Categorical, DataFrame, Series, Index
from ..core.backends.pandas.api.types import is_scalar
from ..core.backends.pandas.core.groupby import SeriesGroupBy

from ..core.contexts import Context
from ..core.utils import arg_match, ensure_nparray
# from .seq import unique


@register_verb(DataFrame, context=Context.EVAL)
def colnames(df, new=None, nested=True):
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
    names = Index([col.split("$", 1)[0] for col in df.columns])
    names = unique(names, __ast_fallback="normal")
    if new is None:
        return names if nested else df.columns.values
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
def rownames(df, new=None):
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
def dim(x, nested=True):
    """Retrieve the dimension of a dataframe.

    Args:
        x: a dataframe
        nested: When there is _nesteded df, count as 1.

    Returns:
        The shape of the dataframe.
    """
    return (
        nrow(x, __ast_fallback="normal"),
        ncol(x, nested, __ast_fallback="normal")
    )


@register_verb(DataFrame)
def nrow(_data) -> int:
    """Get the number of rows in a dataframe

    Args:
        _data: The dataframe

    Returns:
        The number of rows in _data
    """
    return _data.shape[0]


@register_verb(DataFrame)
def ncol(_data, nested=True):
    """Get the number of columns in a dataframe

    Args:
        _data: The dataframe
        nested: When there is _nesteded df, count as 1.

    Returns:
        The number of columns in _data
    """
    if not nested:
        return _data.shape[1]

    return len(colnames(_data, nested=nested, __ast_fallback=True))


@register_verb(object, context=Context.EVAL)
def diag(x=1, nrow=None, ncol=None):
    """Extract, construct a diagonal dataframe or replace the diagnal of
    a dataframe.

    When used with TibbleGrouped data, groups are ignored

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
    x,
    nrow=None,
    ncol=None,
):
    """Diag when x is a dataframe"""
    if nrow is not None and ncol is not None:
        raise ValueError("Extra arguments received for diag.")

    x = x.copy()
    if nrow is not None:
        np.fill_diagonal(x.values, nrow)
        return x
    return np.diag(x)


@register_verb(DataFrame)
def t(_data, copy=False):
    """Get the transposed dataframe

    Args:
        _data: The dataframe
        copy: When copy the data in memory

    Returns:
        The transposed dataframe.
    """
    return _data.transpose(copy=copy)


@register_verb(object, context=Context.EVAL)
def setdiff(x, y):
    """Diff of two iterables"""
    x = ensure_nparray(x)
    y = ensure_nparray(y)
    return np.array([elem for elem in x if elem not in frozenset(y)])


@register_verb(object, context=Context.EVAL)
def intersect(x, y):
    """Intersect of two iterables"""
    # order not kept
    # return np.intersect1d(x, y)
    x = pd.unique(ensure_nparray(x))
    y = ensure_nparray(y)
    return np.array([elem for elem in x if elem in frozenset(y)])


@register_verb(object, context=Context.EVAL)
def union(x, y):
    """Union of two iterables"""
    # order not kept
    # return np.union1d(x, y)
    out = np.concatenate([ensure_nparray(x), ensure_nparray(y)])
    return pd.unique(out)


@register_verb(object, context=Context.EVAL)
def unique(x):
    """Get unique elements from an iterable and keep their order"""
    # order not kept
    # return np.unique(x)
    if is_scalar(x):
        return x
    return pd.unique(x)


@unique.register(SeriesGroupBy)
def _(x):
    return x.apply(pd.unique).explode().astype(x.obj.dtype)


@register_verb(object, context=Context.EVAL)
def setequal(x, y, equal_na=True):
    """Check set equality for two iterables (order doesn't matter)"""
    return np.array_equal(
        np.sort(ensure_nparray(x)),
        np.sort(ensure_nparray(y)),
        equal_nan=equal_na,
    )


@register_verb(
    (np.ndarray, list, tuple, Series, Categorical),
    context=Context.EVAL,
)
def append(x, values, after=-1):
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
    if after is None:
        after = 0
    elif after < 0:
        after += len(x) + 1
    else:
        after += 1
    return np.insert(x, after, values)


@register_verb((list, tuple, np.ndarray, Series, Categorical))
def duplicated(
    x,
    incomparables=None,
    from_last=False,
):
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
    x,
    incomparables=None,
    from_last=False,
):
    """Check if rows in a data frame are duplicated

    `incomparables` not working here
    """
    keep = "first" if not from_last else "last"
    return x.duplicated(keep=keep).values


@register_verb(DataFrame)
def max_col(df, ties_method="random"):
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

    return df.apply(which_max_with_ties, axis=1).values


@register_verb(DataFrame)
def complete_cases(_data):
    """Return a logical vector indicating values of rows are complete.

    Args:
        _data: The dataframe

    Returns:
        A logical vector specifying which observations/rows have no
        missing values across the entire sequence.
    """
    return _data.apply(lambda row: row.notna().all(), axis=1).values


@register_verb(DataFrame)
def proportions(x, margin=None):
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
        sumall = x.values.sum()
        return x.applymap(lambda elem: elem / sumall, na_action="ignore")

    if margin == 1:
        index = x.index
        out = t(
            proportions(
                rename_with(
                    t(x, __ast_fallback="normal"),
                    str,
                    __ast_fallback="normal",
                ),
                2,
                __ast_fallback="normal",
            ),
            __ast_fallback="normal",
        )
        out.index = index
        return out

    if margin == 2:
        return mutate(
            x,
            across(
                x >> everything(),
                lambda col: col / sum_.func(col),
            ),
            __ast_fallback="normal",
        )

    return x.applymap(lambda elem: 1, na_action="ignore")


prop_table = proportions


@proportions.register((list, tuple, np.ndarray, Series))
def _(x, margin=None):
    """proportions for vectors"""
    x = ensure_nparray(x)
    return x / np.sum(x)


# actually from R::utils
@register_verb((DataFrame, Series, list, tuple, np.ndarray))
def head(_data, n=6):
    """Get the first n rows of the dataframe or a vector

    This function will ignore the grouping structure.

    Args:
        n: The number of rows/elements to return

    Returns:
        The dataframe with first n rows or a vector with first n elements
    """
    if isinstance(_data, DataFrame):
        return _data.head(n)
    return _data[:n]


@register_verb((DataFrame, Series, list, tuple, np.ndarray))
def tail(_data, n=6):
    """Get the last n rows of the dataframe or a vector

    This function will ignore the grouping structure.

    Args:
        n: The number of rows/elements to return

    Returns:
        The dataframe with last n rows or a vector with last n elements
        Note that the index is dropped.
    """
    if isinstance(_data, DataFrame):
        return _data.tail(n).reset_index(drop=True)

    out = _data[-n:]
    try:
        return out.reset_index(drop=True)
    except AttributeError:
        return out
