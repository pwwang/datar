# import the variables with _ so that they are not imported by *
from __future__ import annotations as _
from typing import Callable as _Callable, Sequence as _Sequence

from pipda import (
    register_verb as _register_verb,
    register_func as _register_func,
)

from ..core.defaults import f as _f_symbolic
from ..core.utils import (
    NotImplementedByCurrentBackendError as _NotImplementedByCurrentBackendError,
)
from .base import intersect, setdiff, setequal, union  # noqa: F401


@_register_verb(dependent=True)
def across(_data, *args, _names=None, **kwargs):
    """Apply the same transformation to multiple columns

    The original API:
    https://dplyr.tidyverse.org/reference/across.html

    Examples:
        #
        >>> iris >> mutate(across(c(f.Sepal_Length, f.Sepal_Width), round))
            Sepal_Length  Sepal_Width  Petal_Length  Petal_Width    Species
               <float64>    <float64>     <float64>    <float64>   <object>
        0            5.0          4.0           1.4          0.2     setosa
        1            5.0          3.0           1.4          0.2     setosa
        ..           ...          ...           ...          ...        ...

        >>> iris >> group_by(f.Species) >> summarise(
        >>>     across(starts_with("Sepal"), mean)
        >>> )
              Species  Sepal_Length  Sepal_Width
             <object>     <float64>    <float64>
        0      setosa         5.006        3.428
        1  versicolor         5.936        2.770
        2   virginica         6.588        2.974

    Args:
        _data: The dataframe.
        *args: If given, the first 2 elements should be columns and functions
            apply to each of the selected columns. The rest of them will be
            the arguments for the functions.
        _names: A glue specification that describes how to name
            the output columns. This can use `{_col}` to stand for the
            selected column name, and `{_fn}` to stand for the name of
            the function being applied.
            The default (None) is equivalent to `{_col}` for the
            single function case and `{_col}_{_fn}` for the case where
            a list is used for _fns. In such a case, `{_fn}` is 0-based.
            To use 1-based index, use `{_fn1}`
        _fn_context: Defines the context to evaluate the arguments for functions
            if they are plain functions.
            Note that registered functions will use its own context
        **kwargs: Keyword arguments for the functions

    Returns:
        A dataframe with one column for each column and each function.
    """
    raise _NotImplementedByCurrentBackendError("across", _data)


@_register_verb(dependent=True)
def c_across(_data, _cols=None):
    """Apply the same transformation to multiple columns rowwisely

    Args:
        _data: The dataframe
        _cols: The columns

    Returns:
        A rowwise tibble
    """
    raise _NotImplementedByCurrentBackendError("c_across", _data)


@_register_verb(dependent=True)
def if_any(_data, *args, _names=None, **kwargs):
    """Apply the same predicate function to a selection of columns and combine
    the results True if any element is True.

    See Also:
        [`across()`](datar.dplyr.across.across)
    """
    raise _NotImplementedByCurrentBackendError("if_any", _data)


@_register_verb(dependent=True)
def if_all(_data, *args, _names=None, **kwargs):
    """Apply the same predicate function to a selection of columns and combine
    the results True if all elements are True.

    See Also:
        [`across()`](datar.dplyr.across.across)
    """
    raise _NotImplementedByCurrentBackendError("if_all", _data)


@_register_verb()
def arrange(_data, *args, _by_group=False, **kwargs):
    """orders the rows of a data frame by the values of selected columns.

    The original API:
    https://dplyr.tidyverse.org/reference/arrange.html

    Args:
        _data: A data frame
        *series: Variables, or functions of variables.
            Use desc() to sort a variable in descending order.
        _by_group: If TRUE, will sort first by grouping variable.
            Applies to grouped data frames only.
        **kwargs: Name-value pairs that apply with mutate

    Returns:
        An object of the same type as _data.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    raise _NotImplementedByCurrentBackendError("arrange", _data)


@_register_func(pipeable=True, dispatchable=True)
def bind_rows(*data, _id=None, _copy: bool = True, **kwargs):
    """Bind rows of give dataframes

    Original APIs https://dplyr.tidyverse.org/reference/bind.html

    Args:
        *data: Dataframes to combine
        _id: The name of the id columns
        _copy: If `False`, do not copy data unnecessarily.
            Original API does not support this. This argument will be
            passed by to `pandas.concat()` as `copy` argument.
        **kwargs: A mapping of dataframe, keys will be used as _id col.

    Returns:
        The combined dataframe
    """
    raise _NotImplementedByCurrentBackendError("bind_rows")


@_register_func(pipeable=True, dispatchable=True)
def bind_cols(*data, _name_repair="unique", _copy: bool = True):
    """Bind columns of give dataframes

    Note that unlike `dplyr`, mismatched dimensions are allowed and
    missing rows will be filled with `NA`s

    Args:
        *data: Dataframes to bind
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        _copy: If `False`, do not copy data unnecessarily.
            Original API does not support this. This argument will be
            passed by to `pandas.concat()` as `copy` argument.

    Returns:
        The combined dataframe
    """
    raise _NotImplementedByCurrentBackendError("bind_cols")


# context
@_register_func(plain=True)
def cur_column(_data, _name):
    """Get the current column

    Args:
        _data: The dataframe
        _name: The column name

    Returns:
        The current column
    """
    raise _NotImplementedByCurrentBackendError("cur_column")


@_register_verb(dependent=True)
def cur_data(_data):
    """Get the current dataframe

    Args:
        _data: The dataframe

    Returns:
        The current dataframe
    """
    raise _NotImplementedByCurrentBackendError("cur_data", _data)


@_register_verb(dependent=True)
def n(_data):
    """Get the current group size

    Args:
        _data: The dataframe

    Returns:
        The number of rows
    """
    raise _NotImplementedByCurrentBackendError("n", _data)


@_register_verb(dependent=True)
def cur_data_all(_data):
    """Get the current data for the current group including
    the grouping variables

    Args:
        _data: The dataframe

    Returns:
        The current dataframe
    """
    raise _NotImplementedByCurrentBackendError("cur_data_all", _data)


@_register_verb(dependent=True)
def cur_group(_data):
    """Get the current group

    Args:
        _data: The dataframe

    Returns:
        The current group
    """
    raise _NotImplementedByCurrentBackendError("cur_group", _data)


@_register_verb(dependent=True)
def cur_group_id(_data):
    """Get the current group id

    Args:
        _data: The dataframe

    Returns:
        The current group id
    """
    raise _NotImplementedByCurrentBackendError("cur_group_id", _data)


@_register_verb(dependent=True)
def cur_group_rows(_data):
    """Get the current group row indices

    Args:
        _data: The dataframe

    Returns:
        The current group rows
    """
    raise _NotImplementedByCurrentBackendError("cur_group_rows", _data)


# count_tally
@_register_verb()
def count(_data, *args, wt=None, sort=False, name=None, _drop=None, **kwargs):
    """Count the number of rows in each group

    Original API:
    https://dplyr.tidyverse.org/reference/count.html

    Args:
        _data: A data frame
        *args: Variables, or functions of variables.
            Use desc() to sort a variable in descending order.
        wt: A variable or function of variables to weight by.
        sort: If TRUE, the result will be sorted by the count.
        name: The name of the count column.
        _drop: If `False`, keep grouping variables even if they are not used.
            Original API does not support this.
        **kwargs: Name-value pairs that apply with mutate

    Returns:
        A data frame with the same number of rows as the number of groups.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    raise _NotImplementedByCurrentBackendError("count", _data)


@_register_verb()
def tally(_data, wt=None, sort=False, name=None):
    """Count the number of rows in each group

    Original API:
    https://dplyr.tidyverse.org/reference/count.html

    Args:
        _data: A data frame
        wt: A variable or function of variables to weight by.
        sort: If TRUE, the result will be sorted by the count.
        name: The name of the count column.

    Returns:
        A data frame with the same number of rows as the number of groups.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    raise _NotImplementedByCurrentBackendError("tally", _data)


@_register_verb()
def add_count(_data, *args, wt=None, sort=False, name="n", **kwargs):
    """Add a count column to a data frame

    Original API:
    https://dplyr.tidyverse.org/reference/count.html

    Args:
        _data: A data frame
        *args: Variables, or functions of variables.
            Use desc() to sort a variable in descending order.
        wt: A variable or function of variables to weight by.
        sort: If TRUE, the result will be sorted by the count.
        name: The name of the count column.
        **kwargs: Name-value pairs that apply with mutate

    Returns:
        A data frame with the same number of rows as the number of groups.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    raise _NotImplementedByCurrentBackendError("add_count", _data)


@_register_verb()
def add_tally(_data, wt=None, sort=False, name="n"):
    """Add a count column to a data frame

    Original API:
    https://dplyr.tidyverse.org/reference/count.html

    Args:
        _data: A data frame
        wt: A variable or function of variables to weight by.
        sort: If TRUE, the result will be sorted by the count.
        name: The name of the count column.

    Returns:
        A data frame with the same number of rows as the number of groups.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    raise _NotImplementedByCurrentBackendError("add_tally", _data)


# desc
@_register_func(pipeable=True, dispatchable=True)
def desc(x):
    """Transform a vector into a format that will be sorted in descending order

    This is useful within arrange().

    The original API:
    https://dplyr.tidyverse.org/reference/desc.html

    Args:
        x: vector to transform

    Returns:
        The descending order of x
    """
    raise _NotImplementedByCurrentBackendError("desc", x)


# filter
@_register_verb()
def filter_(_data, *conditions, _preserve: bool = False):
    """Filter a data frame based on conditions

    The original API:
    https://dplyr.tidyverse.org/reference/filter.html

    Args:
        _data: A data frame
        *conditions: Conditions to filter by.
        _preserve: If `True`, keep grouping variables even if they are not used.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("filter", _data)


# distinct
@_register_verb()
def distinct(_data, *args, keep_all: bool = False, _preserve: bool = False):
    """Filter a data frame based on conditions

    The original API:
    https://dplyr.tidyverse.org/reference/distinct.html

    Args:
        _data: A data frame
        *args: Variables to filter by.
        keep_all: If `True`, keep all rows that match.
        _preserve: If `True`, keep grouping variables even if they are not used.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("distinct", _data)


@_register_verb()
def n_distinct(_data, na_rm: bool = True):
    """Count the number of distinct values

    The original API:
    https://dplyr.tidyverse.org/reference/distinct.html

    Args:
        _data: A data frame
        na_rm: If `True`, remove missing values before counting.

    Returns:
        The number of distinct values
    """
    raise _NotImplementedByCurrentBackendError("n_distinct", _data)


# glimpse
@_register_verb()
def glimpse(_data, width: int = None, formatter=None):
    """Display a summary of a data frame

    The original API:
    https://dplyr.tidyverse.org/reference/glimpse.html

    Args:
        _data: A data frame
        width: Width of output, defaults to the width of the console.
        formatter: A single-dispatch function to format a single element.
    """
    raise _NotImplementedByCurrentBackendError("glimpse", _data)


# slice
@_register_verb()
def slice_(_data, *args, _preserve: bool = False):
    """Extract rows by their position

    The original API:
    https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: A data frame
        *args: Positions to extract.
        _preserve: If `True`, keep grouping variables even if they are not used.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("slice", _data)


@_register_verb()
def slice_head(_data, n: int = None, prop: float = None):
    """Extract the first rows

    The original API:
    https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: A data frame
        n: Number of rows to extract.
        prop: Proportion of rows to extract.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("slice_head", _data)


@_register_verb()
def slice_tail(_data, n: int = None, prop: float = None):
    """Extract the last rows

    The original API:
    https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: A data frame
        n: Number of rows to extract.
        prop: Proportion of rows to extract.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("slice_tail", _data)


@_register_verb()
def slice_sample(
    _data,
    n: int = 1,
    prop: float = None,
    weight_by=None,
    replace: bool = False,
):
    """Extract rows by sampling

    The original API:
    https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: A data frame
        n: Number of rows to extract.
        prop: Proportion of rows to extract.
        weight_by: A variable or function of variables to weight by.
        replace: If `True`, sample with replacement.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("slice_sample", _data)


@_register_verb()
def slice_min(
    _data,
    order_by,
    n: int = 1,
    prop: float = None,
    with_ties: bool | str = None,
):
    """Extract rows with the minimum value

    The original API:
    https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: A data frame
        order_by: A variable or function of variables to order by.
        n: Number of rows to extract.
        prop: Proportion of rows to extract.
        with_ties: If `True`, extract all rows with the minimum value.
            If "first", extract the first row with the minimum value.
            If "last", extract the last row with the minimum value.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("slice_min", _data)


@_register_verb()
def slice_max(
    _data,
    order_by,
    n: int = 1,
    prop: float = None,
    with_ties: bool | str = None,
):
    """Extract rows with the maximum value

    The original API:
    https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: A data frame
        order_by: A variable or function of variables to order by.
        n: Number of rows to extract.
        prop: Proportion of rows to extract.
        with_ties: If `True`, extract all rows with the maximum value.
            If "first", extract the first row with the maximum value.
            If "last", extract the last row with the maximum value.

    Returns:
        The subset dataframe
    """
    raise _NotImplementedByCurrentBackendError("slice_max", _data)


# misc funs
@_register_func(pipeable=True, dispatchable=True)
def between(x, left, right, inclusive: str = "both"):
    """Check if a value is between two other values

    The original API:
    https://dplyr.tidyverse.org/reference/between.html

    Args:
        x: A value
        left: The left bound
        right: The right bound
        inclusive: Either `both`, `neither`, `left` or `right`.
            Include boundaries. Whether to set each bound as closed or open.

    Returns:
        A bool value if `x` is scalar, otherwise an array of boolean values
        Note that it will be always False when NA appears in x, left or right.
    """
    raise _NotImplementedByCurrentBackendError("between")


@_register_func(pipeable=True, dispatchable=True)
def cummean(x, na_rm: bool = False):
    """Cumulative mean

    The original API:
    https://dplyr.tidyverse.org/reference/cumall.html

    Args:
        x: A numeric vector
        na_rm: If `True`, remove missing values before computing.

    Returns:
        An array of cumulative means
    """
    raise _NotImplementedByCurrentBackendError("cummean", x)


@_register_func(pipeable=True, dispatchable=True)
def cumall(x):
    """Get cumulative bool. All cases after first False

    The original API:
    https://dplyr.tidyverse.org/reference/cumall.html

    Args:
        x: A logical vector

    Returns:
        An array of cumulative conjunctions
    """
    raise _NotImplementedByCurrentBackendError("cumall", x)


@_register_func(pipeable=True, dispatchable=True)
def cumany(x):
    """Get cumulative bool. All cases after first True

    The original API:
    https://dplyr.tidyverse.org/reference/cumany.html

    Args:
        x: A logical vector

    Returns:
        An array of cumulative disjunctions
    """
    raise _NotImplementedByCurrentBackendError("cumany", x)


@_register_func(pipeable=True, dispatchable=True)
def coalesce(x, *replace):
    """Replace missing values with the first non-missing value

    The original API:
    https://dplyr.tidyverse.org/reference/coalesce.html

    Args:
        x: A vector
        *replace: Values to replace missing values with.

    Returns:
        An array of values
    """
    raise _NotImplementedByCurrentBackendError("coalesce")


@_register_func(pipeable=True, dispatchable=True)
def na_if(x, value):
    """Replace values with missing values

    The original API:
    https://dplyr.tidyverse.org/reference/na_if.html

    Args:
        x: A vector
        value: Values to replace with missing values.

    Returns:
        An array of values
    """
    raise _NotImplementedByCurrentBackendError("na_if")


@_register_func(pipeable=True, dispatchable=True)
def near(x, y, tol: float = 1e-8):
    """Check if values are approximately equal

    The original API:
    https://dplyr.tidyverse.org/reference/near.html

    Args:
        x: A numeric vector
        y: A numeric vector
        tol: Tolerance

    Returns:
        An array of boolean values
    """
    raise _NotImplementedByCurrentBackendError("near")


@_register_func(pipeable=True, dispatchable=True)
def nth(x, n, order_by=None, default=None):
    """Extract the nth element of a vector

    The original API:
    https://dplyr.tidyverse.org/reference/nth.html

    Args:
        x: A vector
        n: The index of the element to extract.
        order_by: A variable or function of variables to order by.
        default: A default value to return if `n` is out of bounds.

    Returns:
        A value
    """
    raise _NotImplementedByCurrentBackendError("nth", x)


@_register_func(pipeable=True, dispatchable=True)
def first(x, order_by=None, default=None):
    """Extract the first element of a vector

    The original API:
    https://dplyr.tidyverse.org/reference/nth.html

    Args:
        x: A vector
        order_by: A variable or function of variables to order by.
        default: A default value to return if `x` is empty.

    Returns:
        A value
    """
    raise _NotImplementedByCurrentBackendError("first", x)


@_register_func(pipeable=True, dispatchable=True)
def last(x, order_by=None, default=None):
    """Extract the last element of a vector

    The original API:
    https://dplyr.tidyverse.org/reference/nth.html

    Args:
        x: A vector
        order_by: A variable or function of variables to order by.
        default: A default value to return if `x` is empty.

    Returns:
        A value
    """
    raise _NotImplementedByCurrentBackendError("last", x)


# group_by
@_register_verb()
def group_by(_data, *args, _add: bool = False, _drop: bool = None):
    """Create a grouped frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_by.html

    Args:
        _data: A data frame
        *args: A variable or function of variables to group by.
        _add: If `True`, add grouping variables to an existing group.
        _drop: If `True`, drop grouping variables from the output.

    Returns:
        A grouped frame
    """
    raise _NotImplementedByCurrentBackendError("group_by", _data)


@_register_verb()
def ungroup(_data, *cols: str | int):
    """Remove grouping variables

    The original API:
    https://dplyr.tidyverse.org/reference/ungroup.html

    Args:
        _data: A grouped frame
        *cols: Columns to remove grouping variables from.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("ungroup", _data)


@_register_verb()
def rowwise(_data, *cols: str | int):
    """Create a rowwise frame

    The original API:
    https://dplyr.tidyverse.org/reference/rowwise.html

    Args:
        _data: A data frame
        *cols: Columns to make rowwise.

    Returns:
        A rowwise frame
    """
    raise _NotImplementedByCurrentBackendError("rowwise", _data)


@_register_verb()
def group_by_drop_default(_data):
    """Get the default value of `_drop` of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_by.html

    Args:
        _data: A data frame

    Returns:
        A bool value
    """
    raise _NotImplementedByCurrentBackendError("group_by_drop_default", _data)


@_register_verb()
def group_vars(_data):
    """Get the grouping variables of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_vars.html

    Args:
        _data: A grouped frame

    Returns:
        A list of grouping variables
    """
    raise _NotImplementedByCurrentBackendError("group_vars", _data)


@_register_verb()
def group_indices(_data):
    """Get the group indices of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_indices.html

    Args:
        _data: A grouped frame

    Returns:
        A list of group indices
    """
    raise _NotImplementedByCurrentBackendError("group_indices", _data)


@_register_verb()
def group_keys(_data):
    """Get the group keys of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_keys.html

    Args:
        _data: A grouped frame

    Returns:
        A list of group keys
    """
    raise _NotImplementedByCurrentBackendError("group_keys", _data)


@_register_verb()
def group_size(_data):
    """Get the group sizes of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_size.html

    Args:
        _data: A grouped frame

    Returns:
        A list of group sizes
    """
    raise _NotImplementedByCurrentBackendError("group_size", _data)


@_register_verb()
def group_rows(_data):
    """Get the group rows of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_rows.html

    Args:
        _data: A grouped frame

    Returns:
        A list of group rows
    """
    raise _NotImplementedByCurrentBackendError("group_rows", _data)


@_register_verb()
def group_cols(_data):
    """Get the group columns of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_cols.html

    Args:
        _data: A grouped frame

    Returns:
        A list of group columns
    """
    raise _NotImplementedByCurrentBackendError("group_cols", _data)


@_register_verb()
def group_data(_data):
    """Get the group data of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/group_data.html

    Args:
        _data: A grouped frame

    Returns:
        A list of group data
    """
    raise _NotImplementedByCurrentBackendError("group_data", _data)


@_register_verb()
def n_groups(_data):
    """Get the number of groups of a frame

    The original API:
    https://dplyr.tidyverse.org/reference/n_groups.html

    Args:
        _data: A grouped frame

    Returns:
        An int value
    """
    raise _NotImplementedByCurrentBackendError("n_groups", _data)


@_register_verb()
def group_map(_data, _f, *args, _keep: bool = False, **kwargs):
    """Apply a function to each group

    The original API:
    https://dplyr.tidyverse.org/reference/group_map.html

    Args:
        _data: A grouped frame
        _f: A function to apply to each group.
        *args: Additional arguments to pass to `func`.
        _keep: If `True`, keep the grouping variables in the output.
        **kwargs: Additional keyword arguments to pass to `func`.

    Returns:
        A list of results
    """
    raise _NotImplementedByCurrentBackendError("group_map", _data)


@_register_verb()
def group_modify(_data, _f, *args, _keep: bool = False, **kwargs):
    """Apply a function to each group

    The original API:
    https://dplyr.tidyverse.org/reference/group_modify.html

    Args:
        _data: A grouped frame
        _f: A function to apply to each group.
        *args: Additional arguments to pass to `func`.
        _keep: If `True`, keep the grouping variables in the output.
        **kwargs: Additional keyword arguments to pass to `func`.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("group_modify", _data)


@_register_verb()
def group_split(_data, *args, _keep: bool = False, **kwargs):
    """Split a grouped frame into a list of data frames

    The original API:
    https://dplyr.tidyverse.org/reference/group_split.html

    Args:
        _data: A grouped frame
        *args: Additional arguments to pass to `func`.
        _keep: If `True`, keep the grouping variables in the output.
        **kwargs: Additional keyword arguments to pass to `func`.

    Returns:
        A list of data frames
    """
    raise _NotImplementedByCurrentBackendError("group_split", _data)


@_register_verb()
def group_trim(_data, _drop=None):
    """Remove empty groups

    The original API:
    https://dplyr.tidyverse.org/reference/group_trim.html

    Args:
        _data: A grouped frame
        _drop: See `group_by`.

    Returns:
        A grouped frame
    """
    raise _NotImplementedByCurrentBackendError("group_trim", _data)


@_register_verb()
def group_walk(_data, _f, *args, _keep: bool = False, **kwargs):
    """Apply a function to each group

    The original API:
    https://dplyr.tidyverse.org/reference/group_walk.html

    Args:
        _data: A grouped frame
        _f: A function to apply to each group.
        *args: Additional arguments to pass to `func`.
        **kwargs: Additional keyword arguments to pass to `func`.

    Returns:
        A grouped frame
    """
    raise _NotImplementedByCurrentBackendError("group_walk", _data)


@_register_verb()
def with_groups(_data, _groups, _func, *args, **kwargs):
    """Modify the grouping variables for a single operation.

    Args:
        _data: A data frame
        _groups: columns passed by group_by
            Use None to temporarily ungroup.
        _func: Function to apply to regrouped data.

    Returns:
        The new data frame with operations applied.
    """
    raise _NotImplementedByCurrentBackendError("with_groups", _data)


@_register_func(pipeable=True, dispatchable=True)
def if_else(condition, true, false, missing=None):
    """Where condition is TRUE, the matching value from true, where it's FALSE,
    the matching value from false, otherwise missing.

    Note that NAs will be False in condition if missing is not specified

    Args:
        condition: the conditions
        true: and
        false: Values to use for TRUE and FALSE values of condition.
            They must be either the same length as condition, or length 1.
        missing: If not None, will be used to replace missing values

    Returns:
        A series with values replaced.
    """
    raise _NotImplementedByCurrentBackendError("if_else")


@_register_func(pipeable=True, dispatchable=True)
def case_when(cond, value, *more_cases):
    """Vectorise multiple `if_else()` statements.

    Args:
        cond: A boolean vector
        value: A vector with values to replace
        *more_cases: A list of tuples (cond, value)

    Returns:
        A vector with values replaced.
    """
    raise _NotImplementedByCurrentBackendError("case_when")


# join
@_register_verb()
def inner_join(
    x,
    y,
    by=None,
    copy: bool = False,
    suffix: _Sequence[str] = ("_x", "_y"),
    keep: bool = False,
):
    """Inner join two data frames by matching rows.

    The original API:
    https://dplyr.tidyverse.org/reference/join.html

    Args:
        x: A data frame
        y: A data frame
        by: A list of column names to join by.
            If None, use the intersection of the columns of x and y.
        copy: If True, always copy the data.
        suffix: A tuple of suffixes to apply to overlapping columns.
        keep: If True, keep the grouping variables in the output.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("inner_join", x)


@_register_verb()
def left_join(
    x,
    y,
    by=None,
    copy: bool = False,
    suffix: _Sequence[str] = ("_x", "_y"),
    keep: bool = False,
):
    """Left join two data frames by matching rows.

    The original API:
    https://dplyr.tidyverse.org/reference/join.html

    Args:
        x: A data frame
        y: A data frame
        by: A list of column names to join by.
            If None, use the intersection of the columns of x and y.
        copy: If True, always copy the data.
        suffix: A tuple of suffixes to apply to overlapping columns.
        keep: If True, keep the grouping variables in the output.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("left_join", x)


@_register_verb()
def right_join(
    x,
    y,
    by=None,
    copy: bool = False,
    suffix: _Sequence[str] = ("_x", "_y"),
    keep: bool = False,
):
    """Right join two data frames by matching rows.

    The original API:
    https://dplyr.tidyverse.org/reference/join.html

    Args:
        x: A data frame
        y: A data frame
        by: A list of column names to join by.
            If None, use the intersection of the columns of x and y.
        copy: If True, always copy the data.
        suffix: A tuple of suffixes to apply to overlapping columns.
        keep: If True, keep the grouping variables in the output.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("right_join", x)


@_register_verb()
def full_join(
    x,
    y,
    by=None,
    copy: bool = False,
    suffix: _Sequence[str] = ("_x", "_y"),
    keep: bool = False,
):
    """Full join two data frames by matching rows.

    The original API:
    https://dplyr.tidyverse.org/reference/join.html

    Args:
        x: A data frame
        y: A data frame
        by: A list of column names to join by.
            If None, use the intersection of the columns of x and y.
        copy: If True, always copy the data.
        suffix: A tuple of suffixes to apply to overlapping columns.
        keep: If True, keep the grouping variables in the output.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("full_join", x)


@_register_verb()
def semi_join(
    x,
    y,
    by=None,
    copy: bool = False,
):
    """Semi join two data frames by matching rows.

    The original API:
    https://dplyr.tidyverse.org/reference/join.html

    Args:
        x: A data frame
        y: A data frame
        by: A list of column names to join by.
            If None, use the intersection of the columns of x and y.
        copy: If True, always copy the data.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("semi_join", x)


@_register_verb()
def anti_join(
    x,
    y,
    by=None,
    copy: bool = False,
):
    """Anti join two data frames by matching rows.

    The original API:
    https://dplyr.tidyverse.org/reference/join.html

    Args:
        x: A data frame
        y: A data frame
        by: A list of column names to join by.
            If None, use the intersection of the columns of x and y.
        copy: If True, always copy the data.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("anti_join", x)


@_register_verb()
def nest_join(
    x,
    y,
    by=None,
    copy: bool = False,
    keep: bool = False,
    name=None,
):
    """Nest join two data frames by matching rows.

    The original API:
    https://dplyr.tidyverse.org/reference/join.html

    Args:
        x: A data frame
        y: A data frame
        by: A list of column names to join by.
            If None, use the intersection of the columns of x and y.
        copy: If True, always copy the data.
        keep: If True, keep the grouping variables in the output.
        name: The name of the column to store the nested data frame.

    Returns:
        A data frame
    """
    raise _NotImplementedByCurrentBackendError("nest_join", x)


# lead/lag
@_register_func(pipeable=True, dispatchable=True)
def lead(x, n=1, default=None, order_by=None):
    """Shift a vector by `n` positions.

    The original API:
    https://dplyr.tidyverse.org/reference/lead.html

    Args:
        x: A vector
        n: The number of positions to shift.
        default: The default value to use for positions that don't exist.
        order_by: A vector of column names to order by.

    Returns:
        A vector
    """
    raise _NotImplementedByCurrentBackendError("lead", x)


@_register_func(pipeable=True, dispatchable=True)
def lag(x, n=1, default=None, order_by=None):
    """Shift a vector by `n` positions.

    The original API:
    https://dplyr.tidyverse.org/reference/lag.html

    Args:
        x: A vector
        n: The number of positions to shift.
        default: The default value to use for positions that don't exist.
        order_by: A vector of column names to order by.

    Returns:
        A vector
    """
    raise _NotImplementedByCurrentBackendError("lag", x)


# mutate
@_register_verb()
def mutate(
    _data, *args, _keep: str = "all", _before=None, _after=None, **kwargs
):
    """Add new columns to a data frame.

    The original API:
    https://dplyr.tidyverse.org/reference/mutate.html

    Args:
        _data: A data frame
        _keep: allows you to control which columns from _data are retained
            in the output:
            - "all", the default, retains all variables.
            - "used" keeps any variables used to make new variables;
              it's useful for checking your work as it displays inputs and
              outputs side-by-side.
            - "unused" keeps only existing variables not used to make new
                variables.
            - "none", only keeps grouping keys (like transmute()).
        _before: A list of column names to put the new columns before.
        _after: A list of column names to put the new columns after.
        *args: and
        **kwargs: Name-value pairs. The name gives the name of the column
            in the output. The value can be:
            - A vector of length 1, which will be recycled to the correct
                length.
            - A vector the same length as the current group (or the whole
                data frame if ungrouped).
            - None to remove the column

    Returns:
        An object of the same type as _data. The output has the following
        properties:
        - Rows are not affected.
        - Existing columns will be preserved according to the _keep
            argument. New columns will be placed according to the
            _before and _after arguments. If _keep = "none"
            (as in transmute()), the output order is determined only
            by ..., not the order of existing columns.
        - Columns given value None will be removed
        - Groups will be recomputed if a grouping variable is mutated.
        - Data frame attributes are preserved.
    """
    raise _NotImplementedByCurrentBackendError("mutate", _data)


@_register_verb()
def transmute(_data, *args, _before=None, _after=None, **kwargs):
    """Add new columns to a data frame and remove existing columns
    using mutate with `_keep="none"`.

    The original API:
    https://dplyr.tidyverse.org/reference/mutate.html

    Args:
        _data: A data frame
        _before: A list of column names to put the new columns before.
        _after: A list of column names to put the new columns after.
        *args: and
        **kwargs: Name-value pairs. The name gives the name of the column
            in the output. The value can be:
            - A vector of length 1, which will be recycled to the correct
                length.
            - A vector the same length as the current group (or the whole
                data frame if ungrouped).
            - None to remove the column

    Returns:
        An object of the same type as _data. The output has the following
        properties:
        - Rows are not affected.
        - Existing columns will be preserved according to the _keep
            argument. New columns will be placed according to the
            _before and _after arguments. If _keep = "none"
            (as in transmute()), the output order is determined only
            by ..., not the order of existing columns.
        - Columns given value None will be removed
        - Groups will be recomputed if a grouping variable is mutated.
        - Data frame attributes are preserved.
    """
    raise _NotImplementedByCurrentBackendError("transmute", _data)


# order_by
@_register_func(plain=True)
def order_by(order, call):
    """Order the data by the given order

    Note:
        This function should be called as an argument
        of a verb. If you want to call it regularly, try `with_order()`

    Examples:
        >>> df = tibble(x=c[1:6])
        >>> df >> mutate(y=order_by(c[5:], cumsum(f.x)))
        >>> # df.y:
        >>> # 15, 14, 12, 9, 5

    Args:
        order: An iterable to control the data order
        data: The data to be ordered

    Returns:
        A Function expression for verb to evaluate.
    """
    raise _NotImplementedByCurrentBackendError("order_by")


@_register_func(pipeable=True, dispatchable=True)
def with_order(order, func, x, *args, **kwargs):
    """Control argument and result of a window function

    Examples:
        >>> with_order([5,4,3,2,1], cumsum, [1,2,3,4,5])
        >>> # 15, 14, 12, 9, 5

    Args:
        order: An iterable to order the arugment and result
        func: The window function
        x: The first arugment for the function
        *args: and
        **kwargs: Other arugments for the function

    Returns:
        The ordered result or an expression if there is expression in arguments
    """
    raise _NotImplementedByCurrentBackendError("with_order", order)


# pull
@_register_verb()
def pull(_data, var: str | int = -1, name=None, to=None):
    """Pull a series or a dataframe from a dataframe

    Args:
        _data: The dataframe
        var: The column to pull, either the name or the index
        name: The name of the pulled value
            - If `to` is frame, or the value pulled is data frame, it will be
              the column names
            - If `to` is series, it will be the series name. If multiple names
              are given, only the first name will be used.
            - If `to` is series, but value pulled is a data frame, then a
              dictionary of series with the series names as keys or given `name`
              as keys.
        to: Type of data to return.
            Only works when pulling `a` for name `a$b`
            - series: Return a pandas Series object
              Group information will be lost
              If pulled value is a dataframe, it will return a dict of series,
              with the series names or the `name` provided.
            - array: Return a numpy.ndarray object
            - frame: Return a DataFrame with that column
            - list: Return a python list
            - dict: Return a dict with `name` as keys and pulled value as values
              Only a single column is allowed to pull
            - If not provided: `series` when pulled data has only one columns.
                `dict` if `name` provided and has the same length as the pulled
                single column. Otherwise `frame`.

    Returns:
        The data according to `to`
    """
    raise _NotImplementedByCurrentBackendError("pull", _data)


def row_number(x=_f_symbolic):
    """Get the row number of x

    Note that this function doesn't support piping.

    Args:
        x: The data to get row number
            Defaults to `Symbolic()` so the whole data is used by default
            when called `row_number()`

    Returns:
        The row number
    """
    return row_number_(x, __ast_fallback="normal")


@_register_func(pipeable=True, dispatchable=True)
def row_number_(x):
    raise _NotImplementedByCurrentBackendError("row_number", x)


def ntile(x=_f_symbolic, *, n: int = None):
    """a rough rank, which breaks the input vector into n buckets.
    The size of the buckets may differ by up to one, larger buckets
    have lower rank.

    Note that this function doesn't support piping.

    Args:
        x: The data to get  rownumber
            Defaults to `Symbolic()` so the whole data is used by default
            when called `ntile(n=...)`
        n: The number of groups to divide the data into

    Returns:
        The row number
    """
    return ntile_(x, n=n, __ast_fallback="normal")


@_register_func(pipeable=True, dispatchable=True)
def ntile_(x, *, n: int = None):
    raise _NotImplementedByCurrentBackendError("ntile", x)


def min_rank(x=_f_symbolic, *, na_last: str = "keep"):
    """Get the min rank of x

    Note that this function doesn't support piping.

    Args:
        x: The data to get row number
            Defaults to `Symbolic()` so the whole data is used by default
            when called `min_rank()`
        na_last: How NA values are ranked
            - "keep": NA values are ranked at the end
            - "top": NA values are ranked at the top
            - "bottom": NA values are ranked at the bottom

    Returns:
        The row number
    """
    return min_rank_(x, na_last=na_last, __ast_fallback="normal")


@_register_func(pipeable=True, dispatchable=True)
def min_rank_(x, *, na_last: str = "keep"):
    raise _NotImplementedByCurrentBackendError("min_rank", x)


def dense_rank(x=_f_symbolic, *, na_last: str = "keep"):
    """Get the dense rank of x

    Note that this function doesn't support piping.

    Args:
        x: The data to get row number
            Defaults to `Symbolic()` so the whole data is used by default
            when called `dense_rank()`
        na_last: How NA values are ranked
            - "keep": NA values are ranked at the end
            - "top": NA values are ranked at the top
            - "bottom": NA values are ranked at the bottom

    Returns:
        The row number
    """
    return dense_rank_(x, na_last=na_last, __ast_fallback="normal")


@_register_func(pipeable=True, dispatchable=True)
def dense_rank_(x, *, na_last: str = "keep"):
    raise _NotImplementedByCurrentBackendError("dense_rank", x)


def percent_rank(x=_f_symbolic, *, na_last: str = "keep"):
    """Get the percent rank of x

    Note that this function doesn't support piping.

    Args:
        x: The data to get row number
            Defaults to `Symbolic()` so the whole data is used by default
            when called `percent_rank()`
        na_last: How NA values are ranked
            - "keep": NA values are ranked at the end
            - "top": NA values are ranked at the top
            - "bottom": NA values are ranked at the bottom

    Returns:
        The row number
    """
    return percent_rank_(x, na_last=na_last, __ast_fallback="normal")


@_register_func(pipeable=True, dispatchable=True)
def percent_rank_(x, *, na_last: str = "keep"):
    raise _NotImplementedByCurrentBackendError("percent_rank", x)


def cume_dist(x=_f_symbolic, *, na_last: str = "keep"):
    """Get the cume_dist of x

    Note that this function doesn't support piping.

    Args:
        x: The data to get row number
            Defaults to `Symbolic()` so the whole data is used by default
            when called `cume_dist()`
        na_last: How NA values are ranked
            - "keep": NA values are ranked at the end
            - "top": NA values are ranked at the top
            - "bottom": NA values are ranked at the bottom

    Returns:
        The row number
    """
    return cume_dist_(x, na_last=na_last, __ast_fallback="normal")


@_register_func(pipeable=True, dispatchable=True)
def cume_dist_(x, *, na_last: str = "keep"):
    raise _NotImplementedByCurrentBackendError("cume_dist", x)


# recode
@_register_func(pipeable=True, dispatchable=True)
def recode(_x, *args, _default=None, _missing=None, **kwargs):
    """Recode a vector, replacing elements in it

    Args:
        x: A vector to modify
        *args: and
        **kwargs: replacements
        _default: If supplied, all values not otherwise matched will be
            given this value. If not supplied and if the replacements are
            the same type as the original values in series, unmatched values
            are not changed. If not supplied and if the replacements are
            not compatible, unmatched values are replaced with np.nan.
        _missing: If supplied, any missing values in .x will be replaced
            by this value.

    Returns:
        The vector with values replaced
    """
    raise _NotImplementedByCurrentBackendError("recode")


@_register_func(pipeable=True, dispatchable=True)
def recode_factor(
    _x,
    *args,
    _default=None,
    _missing=None,
    _ordered: bool = False,
    **kwargs,
):
    """Recode a factor, replacing levels in it

    Args:
        x: A factor to modify
        *args: and
        **kwargs: replacements
        _default: If supplied, all values not otherwise matched will be
            given this value. If not supplied and if the replacements are
            the same type as the original values in series, unmatched values
            are not changed. If not supplied and if the replacements are
            not compatible, unmatched values are replaced with np.nan.
        _missing: If supplied, any missing values in .x will be replaced
            by this value.
        _ordered: If True, the factor will be ordered

    Returns:
        The factor with levels replaced
    """
    raise _NotImplementedByCurrentBackendError("recode_factor")


@_register_verb()
def relocate(
    _data,
    *args,
    _before: int | str = None,
    _after: int | str = None,
    **kwargs,
):
    """change column positions

    See original API
    https://dplyr.tidyverse.org/reference/relocate.html

    Args:
        _data: A data frame
        *args: and
        **kwargs: Columns to rename and move
        _before: and
        _after: Destination. Supplying neither will move columns to
            the left-hand side; specifying both is an error.

    Returns:
        An object of the same type as .data. The output has the following
        properties:
        - Rows are not affected.
        - The same columns appear in the output, but (usually) in a
            different place.
        - Data frame attributes are preserved.
        - Groups are not affected
    """
    raise _NotImplementedByCurrentBackendError("relocate", _data)


@_register_verb()
def rename(_data, **kwargs):
    """Rename columns

    See original API
    https://dplyr.tidyverse.org/reference/rename.html

    Args:
        _data: A data frame
        **kwargs: Columns to rename

    Returns:
        The dataframe with new names
    """
    raise _NotImplementedByCurrentBackendError("rename", _data)


@_register_verb()
def rename_with(_data, _fn, *args, **kwargs):
    """Rename columns with a function

    See original API
    https://dplyr.tidyverse.org/reference/rename.html

    Args:
        _data: A data frame
        _fn: A function to apply to column names
        *args: the columns to rename and non-keyword arguments for the `_fn`.
            If `*args` is not provided, then assuming all columns, and
            no non-keyword arguments are allowed to pass to the function, use
            keyword arguments instead.
        **kwargs: keyword arguments for `_fn`

    Returns:
        The dataframe with new names
    """
    raise _NotImplementedByCurrentBackendError("rename_with", _data)


# rows
@_register_verb()
def rows_insert(
    x,
    y,
    by=None,
    conflict: str = "error",
    copy: bool = False,
    in_place: bool = False,
):
    """Insert rows from y into x

    See original API
    https://dplyr.tidyverse.org/reference/rows.html

    Args:
        x: A data frame
        y: A data frame
        by: An unnamed character vector giving the key columns.
            The key columns must exist in both x and y.
            Keys typically uniquely identify each row, but this is only
            enforced for the key values of y
            By default, we use the first column in y, since the first column is
            a reasonable place to put an identifier variable.
        conflict: How to handle conflicts
            - "error": Throw an error
            - "ignore": Ignore conflicts
        copy: If x and y are not from the same data source, and copy is TRUE,
            then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a potentially
            expensive operation so you must opt into it.
        in_place: Should x be modified in place?
            This may not be supported, depending on the backend implementation.

    Returns:
        A data frame with all existing rows and potentially new rows
    """
    raise _NotImplementedByCurrentBackendError("rows_insert", x)


@_register_verb()
def rows_update(
    x,
    y,
    by=None,
    unmatched: str = "error",
    copy: bool = False,
    in_place: bool = False,
):
    """Update rows in x with values from y

    See original API
    https://dplyr.tidyverse.org/reference/rows.html

    Args:
        x: A data frame
        y: A data frame
        by: An unnamed character vector giving the key columns.
            The key columns must exist in both x and y.
            Keys typically uniquely identify each row, but this is only
            enforced for the key values of y
            By default, we use the first column in y, since the first column is
            a reasonable place to put an identifier variable.
        unmatched: how should keys in y that are unmatched by the keys
            in x be handled?
            One of -
            "error", the default, will error if there are any keys in y that
            are unmatched by the keys in x.
            "ignore" will ignore rows in y with keys that are unmatched
            by the keys in x.
        copy: If x and y are not from the same data source, and copy is TRUE,
            then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a potentially
            expensive operation so you must opt into it.
        in_place: Should x be modified in place?
            This may not be supported, depending on the backend implementation.

    Returns:
        A data frame with all existing rows and potentially new rows
    """
    raise _NotImplementedByCurrentBackendError("rows_update", x)


@_register_verb()
def rows_patch(
    x,
    y,
    by=None,
    unmatched: str = "error",
    copy: bool = False,
    in_place: bool = False,
):
    """Patch rows in x with values from y

    See original API
    https://dplyr.tidyverse.org/reference/rows.html

    Args:
        x: A data frame
        y: A data frame
        by: An unnamed character vector giving the key columns.
            The key columns must exist in both x and y.
            Keys typically uniquely identify each row, but this is only
            enforced for the key values of y
            By default, we use the first column in y, since the first column is
            a reasonable place to put an identifier variable.
        unmatched: how should keys in y that are unmatched by the keys
            in x be handled?
            One of -
            "error", the default, will error if there are any keys in y that
            are unmatched by the keys in x.
            "ignore" will ignore rows in y with keys that are unmatched
            by the keys in x.
        copy: If x and y are not from the same data source, and copy is TRUE,
            then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a potentially
            expensive operation so you must opt into it.
        in_place: Should x be modified in place?
            This may not be supported, depending on the backend implementation.

    Returns:
        A data frame with NA values overwritten and the number of rows preserved
    """
    raise _NotImplementedByCurrentBackendError("rows_patch", x)


@_register_verb()
def rows_upsert(
    x,
    y,
    by=None,
    copy: bool = False,
    in_place: bool = False,
):
    """Upsert rows in x with values from y

    See original API
    https://dplyr.tidyverse.org/reference/rows.html

    Args:
        x: A data frame
        y: A data frame
        by: An unnamed character vector giving the key columns.
            The key columns must exist in both x and y.
            Keys typically uniquely identify each row, but this is only
            enforced for the key values of y
            By default, we use the first column in y, since the first column is
            a reasonable place to put an identifier variable.
        copy: If x and y are not from the same data source, and copy is TRUE,
            then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a potentially
            expensive operation so you must opt into it.
        in_place: Should x be modified in place?
            This may not be supported, depending on the backend implementation.

    Returns:
        A data frame with inserted or updated depending on whether or not
        the key value in y already exists in x. Key values in y must be unique.
    """
    raise _NotImplementedByCurrentBackendError("rows_upsert", x)


@_register_verb()
def rows_delete(
    x,
    y,
    by=None,
    unmatched: str = "error",
    copy: bool = False,
    in_place: bool = False,
):
    """Delete rows in x that match keys in y

    See original API
    https://dplyr.tidyverse.org/reference/rows.html

    Args:
        x: A data frame
        y: A data frame
        by: An unnamed character vector giving the key columns.
            The key columns must exist in both x and y.
            Keys typically uniquely identify each row, but this is only
            enforced for the key values of y
            By default, we use the first column in y, since the first column is
            a reasonable place to put an identifier variable.
        unmatched: how should keys in y that are unmatched by the keys
            in x be handled?
            One of -
            "error", the default, will error if there are any keys in y that
            are unmatched by the keys in x.
            "ignore" will ignore rows in y with keys that are unmatched
            by the keys in x.
        copy: If x and y are not from the same data source, and copy is TRUE,
            then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a potentially
            expensive operation so you must opt into it.
        in_place: Should x be modified in place?
            This may not be supported, depending on the backend implementation.

    Returns:
        A data frame with rows deleted
    """
    raise _NotImplementedByCurrentBackendError("rows_delete", x)


@_register_verb()
def rows_append(
    x,
    y,
    copy: bool = False,
    in_place: bool = False,
):
    """Append rows in y to x

    See original API
    https://dplyr.tidyverse.org/reference/rows.html

    Args:
        x: A data frame
        y: A data frame
        copy: If x and y are not from the same data source, and copy is TRUE,
            then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a potentially
            expensive operation so you must opt into it.
        in_place: Should x be modified in place?
            This may not be supported, depending on the backend implementation.

    Returns:
        A data frame with rows appended
    """
    raise _NotImplementedByCurrentBackendError("rows_append", x)


@_register_verb()
def select(_data, *args, **kwargs):
    """Select columns from a data frame.

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    Args:
        _data: A data frame
        *args: A list of columns to select
        **kwargs: A list of columns to select

    Returns:
        A data frame with only the selected columns
    """
    raise _NotImplementedByCurrentBackendError("select", _data)


@_register_func(pipeable=True, dispatchable=True)
def union_all(x, y):
    """Combine two data frames together.

    See original API
    https://dplyr.tidyverse.org/reference/setops.html

    Args:
        x: A data frame
        y: A data frame

    Returns:
        A data frame with rows from x and y
    """
    raise _NotImplementedByCurrentBackendError("union_all", x)


@_register_verb()
def summarise(_data, *args, _groups: str = None, **kwargs):
    """Summarise a data frame.

    See original API
    https://dplyr.tidyverse.org/reference/summarise.html

    Args:
        _data: A data frame
        _groups: Grouping structure of the result.
            - "drop_last": dropping the last level of grouping.
            - "drop": All levels of grouping are dropped.
            - "keep": Same grouping structure as _data.
            - "rowwise": Each row is its own group.
        *args: and
        **kwargs: Name-value pairs, where value is the summarized
            data for each group

    Returns:
        A data frame with the summarised columns
    """
    raise _NotImplementedByCurrentBackendError("summarise", _data)


summarize = summarise


@_register_verb(dependent=True)
def where(_data, fn: _Callable):
    """Selects the variables for which a function returns True.

    See original API
    https://dplyr.tidyverse.org/reference/filter.html

    Args:
        _data: A data frame
        fn: A function that returns True or False.
            Currently it has to be `register_func/func_factory
            registered function purrr-like formula not supported yet.

    Returns:
        The matched columns
    """
    raise _NotImplementedByCurrentBackendError("where", _data)


@_register_verb(dependent=True)
def everything(_data):
    """Select all variables.

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    Args:
        _data: A data frame

    Returns:
        All columns
    """
    raise _NotImplementedByCurrentBackendError("everything", _data)


@_register_verb(dependent=True)
def last_col(_data, offset: int = 0, vars=None):
    """Select the last column.

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    Args:
        _data: A data frame
        offset: The offset of the last column
        vars: A list of columns to select

    Returns:
        The last column
    """
    raise _NotImplementedByCurrentBackendError("last_col", _data)


@_register_verb(dependent=True)
def starts_with(_data, match, ignore_case: bool = True, vars=None):
    """Select columns that start with a string.

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    Args:
        _data: A data frame
        match: The string to match
        ignore_case: Ignore case when matching
        vars: A list of columns to select

    Returns:
        The matched columns
    """
    raise _NotImplementedByCurrentBackendError("starts_with", _data)


@_register_verb(dependent=True)
def ends_with(_data, match, ignore_case: bool = True, vars=None):
    """Select columns that end with a string.

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    Args:
        _data: A data frame
        match: The string to match
        ignore_case: Ignore case when matching
        vars: A list of columns to select

    Returns:
        The matched columns
    """
    raise _NotImplementedByCurrentBackendError("ends_with", _data)


@_register_verb(dependent=True)
def contains(_data, match, ignore_case: bool = True, vars=None):
    """Select columns that contain a string.

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    Args:
        _data: A data frame
        match: The string to match
        ignore_case: Ignore case when matching
        vars: A list of columns to select

    Returns:
        The matched columns
    """
    raise _NotImplementedByCurrentBackendError("contains", _data)


@_register_verb(dependent=True)
def matches(_data, match, ignore_case: bool = True, vars=None):
    """Select columns that match a regular expression.

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    Args:
        _data: A data frame
        match: The regular expression to match
        ignore_case: Ignore case when matching
        vars: A list of columns to select

    Returns:
        The matched columns
    """
    raise _NotImplementedByCurrentBackendError("matches", _data)


@_register_func(pipeable=True, dispatchable=True)
def num_range(prefix: str, range_, width: int = None):
    """Matches a numerical range like x01, x02, x03.

    Args:
        _data: The data piped in
        prefix: A prefix that starts the numeric range.
        range_: A sequence of integers, like `range(3)` (produces `0,1,2`).
        width: Optionally, the "width" of the numeric range.
            For example, a range of 2 gives "01", a range of three "001", etc.

    Returns:
        A list of ranges with prefix.
    """
    raise _NotImplementedByCurrentBackendError("num_range")


@_register_verb(dependent=True)
def all_of(_data, x):
    """For strict selection.

    If any of the variables in the character vector is missing,
    an error is thrown.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns

    Returns:
        The matched column names

    Raises:
        ColumnNotExistingError: When any of the elements in `x` does not exist
            in `_data` columns
    """
    raise _NotImplementedByCurrentBackendError("all_of", _data)


@_register_verb(dependent=True)
def any_of(_data, x, vars=None):
    """For strict selection.

    If any of the variables in the character vector is missing,
    an error is thrown.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns
        vars: A list of columns to select

    Returns:
        The matched column names

    Raises:
        ColumnNotExistingError: When any of the elements in `x` does not exist
            in `_data` columns
    """
    raise _NotImplementedByCurrentBackendError("any_of", _data)
