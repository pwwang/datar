# pylint: disable=too-many-lines
"""Verbs ported from R-dplyr"""
from typing import (
    Any, Callable, Iterable, List, Mapping, Optional, Union
)

import numpy
import pandas
from pandas import DataFrame, Series, RangeIndex, Categorical
from pandas.api.types import union_categoricals
from pandas.core.groupby.generic import DataFrameGroupBy

from pipda import register_verb, evaluate_expr
from pipda.utils import Expression

from ..core.middlewares import Inverted
from ..core.types import (
    DataFrameType, NoneType, NumericOrIter, SeriesLikeType, StringOrIter,
    is_scalar
)
from ..core.contexts import ContextEval, ContextSelectSlice
from ..core.exceptions import ColumnNameInvalidError, ColumnNotExistingError
from ..core.utils import (
    align_value, check_column_uniqueness, copy_flags, df_assign_item,
    expand_slice, get_n_from_prop, group_df, groupby_apply, list_diff,
    list_intersect, list_union, objectize, vars_select, to_df,
    logger, update_df
)
from ..core.names import repair_names
from ..core.contexts import Context
from ..tibble.funcs import tibble
from ..base.funcs import is_categorical
from .funcs import group_by_drop_default

# pylint: disable=redefined-builtin,no-value-for-parameter


def _mutate_rowwise(_data: DataFrame, *args: Any, **kwargs: Any) -> DataFrame:
    """Mutate on rowwise data frame"""
    if _data.shape[0] > 0:
        def apply_func(ser):
            return (ser.to_frame().T >> mutate(*args, **kwargs)).iloc[0, :]

        applied = _data.apply(
            apply_func,
            axis=1
        ).reset_index(drop=True)
    else:
        applied = DataFrame(
            columns=list_union(_data.columns.tolist(), kwargs.keys())
        )
    copy_flags(applied, _data) # rowwise copied
    return applied

@register_verb(DataFrame, context=Context.PENDING)
def mutate(
        _data: DataFrame,
        *args: Any,
        _keep: str = 'all',
        _before: Optional[str] = None,
        _after: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:
    # pylint: disable=too-many-branches
    """Adds new variables and preserves existing ones

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
        _before: and
        _after: Optionally, control where new columns should appear
            (the default is to add to the right hand side).
            See relocate() for more details.
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
    if getattr(_data.flags, 'rowwise', False):
        return _mutate_rowwise(
            _data,
            *args,
            _keep=_keep,
            _before=_before,
            _after=_after,
            **kwargs
        )

    context = ContextEval()

    if _before is not None:
        _before = evaluate_expr(_before, _data, Context.SELECT)
    if _after is not None:
        _after = evaluate_expr(_after, _data, Context.SELECT)

    data = _data.copy()
    copy_flags(data, _data)

    serieses = {} # no need OrderedDict in python3.7+ anymore
    for i, ser in enumerate(args):
        ser = evaluate_expr(ser, data, context)
        if isinstance(ser, Series):
            serieses[ser.name] = ser.values
        elif isinstance(ser, DataFrame):
            serieses.update(ser.to_dict('series'))
        elif isinstance(ser, dict):
            serieses.update(ser)
        else:
            serieses[f'V{i}'] = ser

    serieses.update(kwargs)

    for key, val in serieses.items():
        if val is None:
            data.drop(columns=[key], inplace=True)
            continue

        val = evaluate_expr(val, data, context)
        value = align_value(val, data)
        if isinstance(value, DataFrame):
            if value.shape[1] == 1:
                df_assign_item(
                    data,
                    key if isinstance(value.columns, RangeIndex)
                    else value.columns[0],
                    value.iloc[:, 0]
                )
            else:
                for col in value.columns:
                    df_assign_item(data, f'{key}${col}', value[col])
        else:
            df_assign_item(data, key, value)

    outcols = list(serieses)
    # do the relocate first
    if _before is not None or _after is not None:
        # pylint: disable=no-value-for-parameter
        data = relocate(data, *outcols, _before=_before, _after=_after)

    # do the keep
    used_cols = list(context.used_refs.keys())
    if _keep == 'used':
        data = data[list_union(used_cols, outcols)]
    elif _keep == 'unused':
        unused_cols = list_diff(data.columns, used_cols)
        data = data[list_union(unused_cols, outcols)]
    elif _keep == 'none':
        data = data[outcols]
    # else:
    # raise
    return data

@mutate.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *args: Any,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Mutate on DataFrameGroupBy object"""
    if _data.obj.shape[0] > 0:
        def apply_func(df):
            copy_flags(df, _data)
            # allow group context to work, such as cur_data()
            df.flags.grouper = _data.grouper
            return df >> mutate(*args, **kwargs)

        applied = groupby_apply(_data, apply_func) # index reset
    else:
        applied = DataFrame(
            columns=list_union(_data.obj.columns.tolist(), kwargs.keys())
        )
    return group_df(applied, _data.grouper)

# Forward pipda.Expression for mutate to evaluate
@register_verb((DataFrame, DataFrameGroupBy), context=Context.PENDING)
def transmutate(
        _data: DataFrameType,
        *series: Iterable[Any],
        _before: Optional[str] = None,
        _after: Optional[str] = None,
        **kwargs: Any
) -> DataFrameType:
    """Mutate with _keep='none'

    See mutate().
    """
    return _data >> mutate(
        *series,
        _keep='none',
        _before=_before,
        _after=_after,
        **kwargs
    )

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def relocate(
        _data: DataFrameType,
        column: str,
        *columns: str,
        _before: Optional[Union[int, str]] = None,
        _after: Optional[Union[int, str]] = None,
) -> DataFrameType:
    """change column positions

    Args:
        _data: A data frame
        column: and
        *columns: Columns to move
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
    grouper = _data.grouper if isinstance(_data, DataFrameGroupBy) else None
    _data = objectize(_data)
    all_columns = _data.columns.to_list()
    columns = vars_select(all_columns, column, *columns)
    rest_columns = list_diff(all_columns, columns)
    if _before is not None and _after is not None:
        raise ColumnNameInvalidError(
            'Only one of _before and _after can be specified.'
        )
    if _before is None and _after is None:
        rearranged = columns + rest_columns
    elif _before is not None:
        before_columns = vars_select(rest_columns, _before)
        cutpoint = min(rest_columns.index(bcol) for bcol in before_columns)
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    else: #_after
        after_columns = vars_select(rest_columns, _after)
        cutpoint = max(rest_columns.index(bcol) for bcol in after_columns) + 1
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    ret = _data[rearranged]
    if grouper is not None:
        return group_df(ret, grouper)
    return ret






# ------------------------------
# group data

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def group_keys(
        _data: DataFrameType,
        *cols: str,
        **kwargs: Any,
) -> DataFrame:
    """Get the group keys as a dataframe"""
    if not isinstance(_data, DataFrameGroupBy):
        _data = group_by(_data, *cols, **kwargs)
    group_levels = list(_data.groups.keys())
    return DataFrame(group_levels, columns=_data.grouper.names)

@register_verb(DataFrameGroupBy, context=Context.EVAL)
def group_rows(_data: DataFrameGroupBy) -> List[str]:
    """Returns the rows which each group contains"""
    return _data.grouper.groups

@register_verb(DataFrameGroupBy, context=Context.EVAL)
def group_vars(_data: DataFrameGroupBy) -> List[str]:
    """gives names of grouping variables as character vector"""
    return _data.grouper.names

@group_vars.register(DataFrame, context=Context.EVAL)
def _(_data: DataFrame) -> List[str]:
    """Group vars of DataFrame"""
    return getattr(_data.flags, 'rowwise', None) or []

group_cols = group_vars # pylint: disable=invalid-name

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def group_map(
        _data: DataFrameType,
        func: Callable[[DataFrame], Any]
) -> List[Any]:
    """Map function to data in each group, returns a list"""
    if isinstance(_data, DataFrame):
        return func(_data)
    return [
        func(_data.obj.loc[index]) for index in _data.grouper.groups.values()
    ]

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def group_modify(
        _data: DataFrameType,
        func: Callable[[DataFrame], DataFrame]
) -> DataFrame:
    """Modify data in each group with func, returns a dataframe"""
    if isinstance(_data, DataFrame):
        return func(_data)
    return _data.apply(func).reset_index(drop=True, level=0)

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def group_walk(
        _data: DataFrameType,
        func: Callable[[DataFrame], None]
) -> None:
    """Walk along data in each groups, but don't return anything"""
    if isinstance(_data, DataFrame):
        func(_data)
    _data.apply(func)

@register_verb(DataFrameGroupBy, context=Context.EVAL)
def group_trim(
        _data: DataFrameGroupBy
) -> DataFrameGroupBy:
    """Trim the unused group levels"""
    return group_df(_data.obj, _data.grouper.names)

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def group_split(
        _data: DataFrameType,
        *cols: str,
        _keep: bool = False,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Get a list of data in each group"""
    if isinstance(_data, DataFrameGroupBy):
        return [
            _data.obj.loc[index] for index in _data.grouper.groups.values()
        ]

    if getattr(_data.flags, 'rowwise', None):
        return [_data.iloc[[i], :] for i in range(_data.shape[0])]

    _data = group_by(_data, *cols, **kwargs)
    return group_split(_data)

@register_verb((DataFrame, DataFrameGroupBy), context=Context.PENDING)
def with_groups(
        _data: DataFrameType,
        _groups: Optional[StringOrIter],
        _func: Callable,
        *args: Any,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Modify the grouping variables for a single operation.

    Args:
        _data: A data frame
        _groups: columns passed by group_by
            Use None to temporarily ungroup.
        _func: Function to apply to regrouped data.

    Returns:
        The new data frame with operations applied.
    """
    _groups = evaluate_expr(_groups, _data, Context.SELECT)
    if _groups is not None:
        _data = group_by(_data, _groups)
    else:
        _data = objectize(_data)

    if getattr(_func, '__pipda__', None):
        return _data >> _func(*args, **kwargs)

    return _func(_data, *args, **kwargs)

def _summarise_rowwise(
        _data: DataFrame,
        *dfs: Union[DataFrame, Mapping[str, Iterable[Any]]],
        _groups: Optional[str] = None,
        **kwargs: Any
) -> DataFrameType:
    """Summarise on rowwise dataframe"""
    rowwise_vars = _data.flags.rowwise

    def apply_func(ser):
        row = ser.to_frame().T.reset_index(drop=True)
        summarised = row >> summarise(*dfs, **kwargs)
        summarised.reset_index(drop=True, inplace=True)
        if rowwise_vars and rowwise_vars is not True:
            ret = row[rowwise_vars].iloc[range(summarised.shape[0]), :]
            ret[summarised.columns.tolist()] = summarised
            return ret

        return summarised

    if _data.shape[0] == 0:
        columns = list(kwargs)
        if rowwise_vars and rowwise_vars is not True:
            columns = list_union(rowwise_vars, columns)
        applied = DataFrame(columns=columns)
    else:
        applied = pandas.concat(
            (apply_func(row[1]) for row in _data.iterrows()),
            axis=0
        )
    copy_flags(applied, _data)
    applied.flags.rowwise = False

    if rowwise_vars is True:
        if _groups == 'rowwise':
            applied.flags.rowwise = True
            return applied

        if _groups is None and summarise.inform:
            logger.info(
                '`summarise()` has ungrouped output. '
                'You can override using the `_groups` argument.'
            )

        return applied

    # rowwise vars set
    if _groups == 'rowwise':
        applied.flags.rowwise = True
        return applied

    if _groups is None and summarise.inform:
        logger.info(
            '`summarise()` has grouped output by %s. '
            'You can override using the `_groups` argument.',
            rowwise_vars
        )
        _groups = 'keep'

    if _groups == 'keep':
        return group_df(applied, rowwise_vars)

    return applied





# ------------------------------
# count

@register_verb(DataFrame, context=Context.PENDING)
def distinct(
        _data: DataFrame,
        *columns: Any,
        _keep_all: bool = False,
        **mutates: Any
) -> DataFrame:
    """Select only unique/distinct rows from a data frame.

    The original API:
    https://dplyr.tidyverse.org/reference/distinct.html

    Args:
        _data: The dataframe
        *columns: and
        **mutates: Optional variables to use when determining
            uniqueness.
        _keep_all: If TRUE, keep all variables in _data

    Returns:
        A dataframe without duplicated rows in _data
    """
    mutated = _data >> mutate(*columns, **mutates, _keep='none')
    data = _data.copy()
    update_df(data, mutated)
    copy_flags(data, _data)

    columns = (
        mutated.columns.tolist()
        if mutated.shape[1] > 0
        else data.columns.tolist()
    )
    # keep the order
    columns = [col for col in data.columns if col in columns]

    data.drop_duplicates(columns, inplace=True)
    if not _keep_all:
        data2 = data[columns]
        copy_flags(data2, data)
        data = data2

    return data

@distinct.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *columns: Any,
        _keep_all: bool = False,
        **mutates: Any
) -> DataFrameGroupBy:

    def apply_func(df):
        return df >> distinct(*columns, _keep_all=_keep_all, **mutates)

    return group_df(
        groupby_apply(_data, apply_func, groupdata=True),
        _data.grouper.names
    )

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def pull(
        _data: DataFrameType,
        var: Union[int, str] = -1,
        name: Optional[StringOrIter] = None,
        to: str = 'series'
) -> SeriesLikeType:
    """Pull a series or a dataframe from a dataframe

    Args:
        _data: The dataframe
        var: The column to pull
        name: If specified, a zip object will be return with the name-value
            pairs. It can be a column name or a list of strs with the same
            length as the series
            Only works when pulling `a` for name `a$b`
        to: Type of data to return.
            Only works when pulling `a` for name `a$b`
            - series: Return a pandas Series object
              Group information will be lost
            - array: Return a numpy.ndarray object
            - list: Return a python list

    Returns:
        The series data.
    """
    _data = objectize(_data)
    if isinstance(var, int):
        var = _data.columns[var]

    # check if var is a dataframe
    if var not in _data:
        cols = [col for col in _data.columns if col.startswith(f'{var}$')]
        ret = _data.loc[:, cols]
        ret.columns = [col[(len(var)+1):] for col in cols]
        return ret

    value = _data[var]
    if to == 'list':
        value = value.values.tolist()
    if to == 'array':
        value = value.values

    if name is not None and is_scalar(name):
        return zip(_data[name].values, value)
    if name is not None:
        return zip(name, value)
    return value

@register_verb(DataFrame, context=Context.SELECT)
def rename(
        _data: DataFrame,
        **kwargs: str
) -> DataFrame:
    """Changes the names of individual variables using new_name = old_name
    syntax

    Args:
        _data: The dataframe
        **kwargs: The new_name = old_name pairs

    Returns:
        The dataframe with new names
    """
    names = {val: key for key, val in kwargs.items()}
    ret = _data.rename(columns=names)
    copy_flags(ret, _data)
    row_wise = getattr(ret.flags, 'rowwise', None)
    if is_scalar(row_wise):
        return ret

    for i, var in enumerate(row_wise):
        if var in names:
            row_wise[i] = names[var]
    return ret

@register_verb(DataFrame, context=Context.SELECT)
def rename_with(
        _data: DataFrame,
        _fn: Callable[[str], str],
        _cols: Optional[Iterable[str]] = None
) -> DataFrame:
    """Renames columns using a function.

    Args:
        _data: The dataframe
        _fn: The function to rename a column
        _cols: the columns to rename. If not specified, all columns are
            considered

    Returns:
        The dataframe with new names
    """
    _cols = _cols or _data.columns

    new_columns = {col: _fn(col) for col in _cols}
    return _data.rename(columns=new_columns)

@register_verb(DataFrame, context=ContextSelectSlice())
def slice(
        _data: DataFrame,
        rows: Any,
        _preserve: bool = False
) -> DataFrame:
    """Index rows by their (integer) locations

    Args:
        _data: The dataframe
        rows: The indexes
        _preserve: Relevant when the _data input is grouped.
            If _preserve = FALSE (the default), the grouping structure is
            recalculated based on the resulting data,
            otherwise the grouping is kept as is.

    Returns:
        The sliced dataframe
    """
    rows = expand_slice(rows, _data.shape[0])
    try:
        ret = _data.iloc[rows, :]
    except IndexError:
        ret = _data.iloc[[], :]

    copy_flags(ret, _data)
    return ret

@slice.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        rows: Any,
        _preserve: bool = False
) -> DataFrameGroupBy:
    """Slice on grouped dataframe"""

    def apply_func(df):
        ret = df >> slice(rows)
        return ret

    grouper = _data.grouper
    if not _preserve:
        grouper = grouper.names

    applied = groupby_apply(_data, apply_func, groupdata=True)
    return group_df(applied, grouper)

@register_verb(DataFrame, context=Context.EVAL)
def slice_head(
        _data: DataFrame,
        n: Optional[int] = None,
        prop: Optional[float] = None
) -> DataFrame:
    """Select first rows

    Args:
        _data: The dataframe.
        n, prop: Provide either n, the number of rows, or prop,
            the proportion of rows to select. If neither are supplied,
            n = 1 will be used.
            If n is greater than the number of rows in the group (or prop > 1),
            the result will be silently truncated to the group size.
            If the proportion of a group size is not an integer,
            it is rounded down.

    Returns:
        The sliced dataframe
    """
    n = get_n_from_prop(_data.shape[0], n, prop)
    rows = list(range(n))
    return _data.iloc[rows, :]

@slice_head.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        n: Optional[Union[int, Iterable[int]]] = None,
        prop: Optional[Union[float, Iterable[float]]] = None
) -> DataFrame:
    """slice_head on grouped dataframe"""
    def apply_func(df):
        return df >> slice_head(n=n, prop=prop)

    applied = groupby_apply(_data, apply_func, groupdata=True)
    return group_df(applied, _data.grouper.names)

@register_verb(DataFrame, context=Context.EVAL)
def slice_tail(
        _data: DataFrame,
        n: Optional[int] = 1,
        prop: Optional[float] = None
) -> DataFrame:
    """Select last rows

    See: slice_head()
    """
    n = get_n_from_prop(_data.shape[0], n, prop)
    rows = [-(elem+1) for elem in range(n)]
    return _data.iloc[rows, :]

@slice_tail.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        n: Optional[Union[int, Iterable[int]]] = None,
        prop: Optional[Union[float, Iterable[float]]] = None
) -> DataFrame:
    """slice_tail on grouped dataframe"""
    def apply_func(df):
        return df >> slice_tail(n=n, prop=prop)

    applied = groupby_apply(_data, apply_func, groupdata=True)
    return group_df(applied, _data.grouper.names)

@register_verb(DataFrame, context=Context.EVAL)
def slice_min(
        _data: DataFrame,
        order_by: Series,
        n: Optional[int] = 1,
        prop: Optional[float] = None,
        with_ties: Union[bool, str] = True
) -> DataFrame:
    """select rows with lowest values of a variable.

    See slice_head()
    """
    data = _data.assign(**{'__slice_order__': order_by.values})
    n = get_n_from_prop(_data.shape[0], n, prop)

    keep = {True: 'all', False: 'first'}.get(with_ties, with_ties)
    return data.nsmallest(n, '__slice_order__', keep).drop(
        columns=['__slice_order__']
    )

@slice_min.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        order_by: Series,
        n: Optional[int] = 1,
        prop: Optional[float] = None,
        with_ties: Union[bool, str] = True
) -> DataFrameGroupBy:
    """slice_min for DataFrameGroupBy object"""
    def apply_func(df):
        df.flags.grouper = _data.grouper
        return df >> slice_min(order_by, n, prop, with_ties)

    return groupby_apply(_data, apply_func)

@register_verb(DataFrame, context=Context.EVAL)
def slice_max(
        _data: DataFrame,
        order_by: Series,
        n: Optional[int] = 1,
        prop: Optional[float] = None,
        with_ties: Union[bool, str] = True
) -> DataFrame:
    """select rows with highest values of a variable.

    See slice_head()
    """
    data = _data.assign(**{'__slice_order__': order_by.values})
    n = get_n_from_prop(_data.shape[0], n, prop)

    keep = {True: 'all', False: 'first'}.get(with_ties, with_ties)
    return data.nlargest(n, '__slice_order__', keep).drop(
        columns=['__slice_order__']
    )

@slice_max.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        order_by: Series,
        n: Optional[int] = 1,
        prop: Optional[float] = None,
        with_ties: Union[bool, str] = True
) -> DataFrameGroupBy:
    """slice_max for DataFrameGroupBy object"""
    def apply_func(df):
        df.flags.grouper = _data.grouper
        return df >> slice_max(order_by, n, prop, with_ties)

    return groupby_apply(_data, apply_func)

@register_verb(DataFrame, context=Context.EVAL)
def slice_sample(
        _data: DataFrame,
        n: Optional[int] = 1,
        prop: Optional[float] = None,
        weight_by: Optional[Iterable[Union[int, float]]] = None,
        replace: bool = False,
        random_state: Any = None
) -> DataFrame:
    """Randomly selects rows.

    See slice_head()
    """
    n = get_n_from_prop(_data.shape[0], n, prop)

    return _data.sample(
        n=n,
        replace=replace,
        weights=weight_by,
        random_state=random_state,
        axis=0
    )

@slice_sample.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        n: Optional[int] = 1,
        prop: Optional[float] = None,
        weight_by: Optional[Iterable[Union[int, float]]] = None,
        replace: bool = False,
        random_state: Any = None
) -> DataFrameGroupBy:

    def apply_func(df):
        df.flags.grouper = _data.grouper
        return df >> slice_sample(n, prop, weight_by, replace, random_state)

    return groupby_apply(_data, apply_func)


# Two table verbs
# ---------------

@register_verb(
    (DataFrame, list, dict, NoneType),
    context=Context.EVAL
)
def bind_rows(
        _data: Optional[Union[DataFrame, list, dict]],
        *datas: Optional[Union[DataFrameType, dict]],
        _id: Optional[str] = None,
        **kwargs: Union[DataFrame, dict]
) -> DataFrame:
    # pylint: disable=too-many-branches
    """Bind rows of give dataframes

    Args:
        _data: The seed dataframe to bind others
            Could be a dict or a list, keys/indexes will be used for _id col
        *datas: Other dataframes to combine
        _id: The name of the id columns
        **kwargs: A mapping of dataframe, keys will be used as _id col.

    Returns:
        The combined dataframe
    """
    if _id is not None and not isinstance(_id, str):
        raise ValueError("`_id` must be a scalar string.")

    def data_to_df(data):
        """Make a copy of dataframe or convert dict to a dataframe"""
        if isinstance(data, (DataFrame, DataFrameGroupBy)):
            return objectize(data).copy()

        ret = tibble(**data) # avoid varname error
        return ret

    key_data = {}
    if isinstance(_data, list):
        for i, dat in enumerate(_data):
            if dat is not None:
                key_data[i] = data_to_df(dat)
    elif _data is not None:
        key_data[0] = data_to_df(_data)

    for i, dat in enumerate(datas):
        if dat is not None:
            key_data[len(key_data)] = data_to_df(dat)

    for key, val in kwargs.items():
        if val is not None:
            key_data[key] = data_to_df(val)

    if not key_data:
        return DataFrame()

    # handle categorical data
    for col in list(key_data.values())[0].columns:
        all_series = [
            dat[col] for dat in key_data.values()
            if col in dat and not dat[col].isna().all()
        ]
        all_categorical = [
            is_categorical(ser) for ser in all_series
        ]
        if all(all_categorical):
            union_cat = union_categoricals(all_series)
            for data in key_data.values():
                if col not in data: # in case it is 0-column df
                    continue
                data[col] = Categorical(
                    data[col],
                    categories=union_cat.categories,
                    ordered=is_categorical(data[col]) and data[col].cat.ordered
                )
        elif any(all_categorical):
            logger.warning("Factor information lost during rows binding.")

    if _id is not None:
        return pandas.concat(
            key_data.values(),
            keys=key_data.keys(),
            names=[_id, None]
        ).reset_index(level=0).reset_index(drop=True)
    return pandas.concat(key_data.values()).reset_index(drop=True)

@bind_rows.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *datas: Optional[Union[DataFrameType, dict]],
        _id: Optional[str] = None,
        **kwargs: Union[DataFrame, dict]
) -> DataFrameGroupBy:

    data = _data.obj >> bind_rows(*datas, _id=_id, **kwargs)
    copy_flags(data, _data)
    return group_df(data, _data.grouper.names)

@register_verb((DataFrame, dict, NoneType), context=Context.EVAL)
def bind_cols(
        _data: Optional[Union[DataFrame, dict]],
        *datas: Optional[Union[DataFrame, dict]],
        _name_repair: Union[str, Callable] = "unique"
) -> DataFrame:
    """Bind columns of give dataframes

    Args:
        _data, *datas: Dataframes to combine

    Returns:
        The combined dataframe
    """
    if isinstance(_data, dict):
        _data = tibble(**_data)
    more_data = []
    for data in datas:
        if isinstance(data, dict):
            more_data.append(tibble(**data))
        else:
            more_data.append(data)
    if _data is not None:
        more_data.insert(0, _data)
    if not more_data:
        return DataFrame()
    ret = pandas.concat(more_data, axis=1)
    ret.columns = repair_names(ret.columns.tolist(), repair=_name_repair)
    return ret

@register_verb(DataFrame, context=Context.EVAL)
def intersect(
        _data: DataFrame,
        data2: DataFrame,
        *datas: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Intersect of two dataframes

    Args:
        _data, data2, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of intersect of input dataframes
    """
    if not on:
        on = _data.columns.to_list()

    return pandas.merge(
        _data,
        data2,
        *datas,
        on=on,
        how='inner'
    ) >> distinct(*on)

@register_verb(DataFrame, context=Context.EVAL)
def union(
        _data: DataFrame,
        data2: DataFrame,
        *datas: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Union of two dataframes

    Args:
        _data, data2, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of union of input dataframes
    """
    if not on:
        on = _data.columns.to_list()

    return pandas.merge(
        _data,
        data2,
        *datas,
        on=on,
        how='outer'
    ) >> distinct(*on)

@register_verb(DataFrame, context=Context.EVAL)
def setdiff(
        _data: DataFrame,
        data2: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Set diff of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of setdiff of input dataframes
    """
    if not on:
        on = _data.columns.to_list()

    return _data.merge(
        data2,
        how='outer',
        on=on,
        indicator=True
    ).loc[
        lambda x: x['_merge'] == 'left_only'
    ].drop(columns=['_merge']) >> distinct(*on)

@register_verb(DataFrame, context=Context.EVAL)
def union_all(
        _data: DataFrame,
        data2: DataFrame
) -> DataFrame:
    """Union of all rows of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of union of all rows of input dataframes
    """
    return bind_rows(_data, data2)

@register_verb(DataFrame, context=Context.EVAL)
def setequal(
        _data: DataFrame,
        data2: DataFrame
) -> bool:
    """Check if two dataframes equal

    Args:
        _data: The first dataframe
        data2: The second dataframe

    Returns:
        True if they equal else False
    """
    data1 = _data.sort_values(by=_data.columns.to_list()).reset_index(drop=True)
    data2 = data2.sort_values(by=data2.columns.to_list()).reset_index(drop=True)
    return data1.equals(data2)

def _join(
        x: DataFrameType,
        y: DataFrameType,
        how: str,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrameType:
    """General join"""
    xobj = objectize(x)
    y = objectize(y)
    if isinstance(by, dict):
        right_on = list(by.values())
        ret = pandas.merge(
            xobj, y,
            left_on=list(by.keys()),
            right_on=right_on,
            how=how,
            copy=copy,
            suffixes=suffix
        )
        if not keep:
            ret.drop(columns=right_on, inplace=True)
    else:
        ret = pandas.merge(
            xobj, y,
            on=by,
            how=how,
            copy=copy,
            suffixes=suffix
        )

    copy_flags(ret, x)
    if isinstance(x, DataFrameGroupBy):
        return group_df(ret, x.grouper.names)

    return ret

@register_verb(
    (DataFrame, DataFrameGroupBy),
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def inner_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in x and y.

    Args:
        x, y: A pair of data frames
        by: A character vector of variables to join by.
        copy: If x and y are not from the same data source, and copy is
            TRUE, then y will be copied into the same src as x.
            This allows you to join tables across srcs, but it is a
            potentially expensive operation so you must opt into it.
        suffix: If there are non-joined duplicate variables in x and y,
            these suffixes will be added to the output to disambiguate them.
            Should be a character vector of length 2.
        keep: Should the join keys from both x and y be preserved in the output?

    Returns:
        The joined dataframe
    """
    return _join(
        x, y,
        how='inner',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(
    (DataFrame, DataFrameGroupBy),
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def left_join(
        x: DataFrameType,
        y: DataFrameType,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrameType:
    """Mutating joins including all rows in x.

    See inner_join()
    """
    return _join(
        x, y,
        how='left',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(
    (DataFrame, DataFrameGroupBy),
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def right_join(
        x: DataFrameType,
        y: DataFrameType,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrameType:
    """Mutating joins including all rows in y.

    See inner_join()
    """
    return _join(
        x, y,
        how='right',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(
    (DataFrame, DataFrameGroupBy),
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def full_join(
        x: DataFrameType,
        y: DataFrameType,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrameType:
    """Mutating joins including all rows in x or y.

    See inner_join()
    """
    return _join(
        x, y,
        how='outer',
        by=by,
        copy=copy,
        suffix=suffix,
        keep=keep
    )

@register_verb(DataFrame, context=Context.EVAL)
def nest_join(
        x: DataFrame,
        y: DataFrame,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False,
        suffix: Iterable[str] = ("_x", "_y"),
        keep: bool = False
) -> DataFrame:
    """Returns all rows and columns in x with a new nested-df column that
    contains all matches from y

    See inner_join()
    """
    on = by
    if isinstance(by, (list, tuple, set)):
        on = dict(zip(by, by))
    elif by is None:
        common_cols = list_intersect(x.columns.tolist(), y.columns)
        on = dict(zip(common_cols, common_cols))
    elif not isinstance(by, dict):
        on = {by: by}

    if copy:
        x = x.copy()

    def get_nested_df(row: Series) -> DataFrame:
        condition = None
        for key in on:
            if condition is None:
                condition = y[on[key]] == row[key]
            else:
                condition = condition and (y[on[key]] == row[key])
        df = filter(y, condition)
        if not keep:
            df = df[list_diff(df.columns.tolist(), on.values())]
        if suffix:
            for col in df.columns:
                if col in x:
                    x.rename(columns={col: f'{col}{suffix[0]}'}, inplace=True)
                    df.rename(columns={col: f'{col}{suffix[1]}'}, inplace=True)
        return df

    y_matched = x.apply(get_nested_df, axis=1)
    y_name = getattr(y, '__dfname__', None)
    if y_name:
        y_matched = y_matched.to_frame(name=y_name)
    return pandas.concat([x, y_matched], axis=1)
