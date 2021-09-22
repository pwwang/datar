"""Function from R-base that can be used as verbs"""
from typing import Any, Iterable, List, Mapping, Sequence, Tuple, Union

import numpy
from pandas import Categorical, DataFrame, Series
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.types import ArrayLikeType, IntType, is_scalar
from ..core.utils import Array, arg_match, get_option, position_after


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
            return set_names(df, new, __calling_env=CallingEnvs.REGULAR)
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
        return set_names(df, newnames, __calling_env=CallingEnvs.REGULAR)

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
    return (
        nrow(x, __calling_env=CallingEnvs.REGULAR),
        ncol(x, _nested, __calling_env=CallingEnvs.REGULAR),
    )


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
    nrow: IntType = None,
    ncol: IntType = None,
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
    nrow: Any = None,
    ncol: IntType = None,
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
    return colnames(x, new, _nested, __calling_env=CallingEnvs.REGULAR)


@names.register(dict)
def _(
    x: Mapping[str, Any], new: Iterable[str] = None, _nested: bool = True
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

    return list(x) + setdiff(y, x, __calling_env=CallingEnvs.REGULAR)


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


@register_verb(
    (list, tuple, numpy.ndarray, Series, Categorical),
    context=Context.EVAL,
)
def append(x: Any, values: Any, after: int = -1, base0_: bool = None) -> List:
    """Add elements to a vector.

    Args:
        x: the vector the values are to be appended to.
        values: to be included in the modified vector.
        after: a subscript, after which the values are to be appended.
        base0_: Whether after is 0-based.
            if not given, will use `get_option("index.base.0")`.
            When it's 1-based, after=0 will append to the beginning,
            -1 will append to the end.
            When 0-based, after=None will append to the beginning,
            -1 to the end

    Returns:
        A vector containing the values in ‘x’ with the elements of
        ‘values’ appended after the specified element of ‘x’.
    """
    # if is_scalar(x):
    #     x = [x]
    if is_scalar(values):
        values = [values]
    x = list(x)
    values = list(values)

    base0_ = get_option("index.base.0", base0_)
    # 0 is not allowed with 1-base
    if base0_ and after is None:
        return values + x
    pos = position_after(after, len(x), base0_)
    return x[:pos] + values + x[pos:]


@register_verb((list, tuple, numpy.ndarray, Series, Categorical))
def duplicated(
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
def _(
    x: DataFrame,
    incomparables: Iterable[Any] = None,
    from_last: bool = False,
) -> numpy.ndarray:
    """Check if rows in a data frame are duplicated

    `incomparables` not working here
    """
    keep = "first" if not from_last else "last"
    return x.duplicated(keep=keep).values


@register_verb(DataFrame)
def max_col(
    df: DataFrame, ties_method: str = "random", base0_: bool = None
) -> Iterable[int]:
    """Find the maximum position for each row of a matrix

    Args:
        df: The data frame
        ties_method: how ties are handled
            - "random": use a random index
            - "first" use the first index
            - "last" use the last index
        base0_: Whether the returned indices are 0-based
            If not provided, will use `get_option("which_base_0")`

    Returns:
        The indices of max values for each row
    """
    ties_method = arg_match(
        ties_method, "ties_method", ["random", "first", "last"]
    )
    base = int(not get_option("which_base_0", base0_))

    def which_max_with_ties(ser: Series):
        """Find index with max if ties happen"""
        indices = numpy.flatnonzero(ser == max(ser)) + base
        if len(indices) == 1 or ties_method == "first":
            return indices[0]
        if ties_method == "random":
            return numpy.random.choice(indices)
        return indices[-1]

    return df.apply(which_max_with_ties, axis=1).to_numpy()


@register_verb(DataFrame)
def complete_cases(_data: DataFrame) -> Iterable[bool]:
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
        sumall = x.to_numpy().sum()
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


@proportions.register((list, tuple, numpy.array, Series))
def _(
    x: ArrayLikeType, margin: Union[int, tuple, list] = None
) -> ArrayLikeType:
    """proportions for vectors"""
    from . import sum_

    if isinstance(x, (list, tuple)):
        x = Array(x)

    return x / sum_(x, __calling_env=CallingEnvs.REGULAR)
