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
    list_intersect, list_union, objectize, select_columns, to_df,
    logger, update_df
)
from ..core.names import repair_names
from ..core.contexts import Context
from ..tibble.funcs import tibble
from ..base.funcs import is_categorical
from .funcs import group_by_drop_default

# pylint: disable=redefined-builtin,no-value-for-parameter

@register_verb(DataFrame, context=Context.EVAL)
def arrange(
        _data: DataFrame,
        *series: Iterable[Any],
        _by_group: bool = False
) -> DataFrame:
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
    if not series:
        return _data

    check_column_uniqueness(
        _data,
        "Cannot arrange a data frame with duplicate names."
    )

    sorting_df = DataFrame(index=_data.index) >> mutate(*series)
    by = sorting_df.columns.tolist()
    sorting_df.sort_values(by=by, inplace=True)

    ret = _data.loc[sorting_df.index, :]
    copy_flags(ret, _data)
    return ret

@arrange.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *series: Any,
        _by_group: bool = False
) -> DataFrameGroupBy:
    """Arrange grouped dataframe"""
    if not _by_group:
        ret = _data.obj >> arrange(*series)
    else:
        ret = _data.obj >> arrange(
            *(_data.obj[col] for col in _data.grouper.names),
            *series
        )
    copy_flags(ret, _data)
    return group_df(ret, _data.grouper.names)

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
    columns = select_columns(all_columns, column, *columns)
    rest_columns = list_diff(all_columns, columns)
    if _before is not None and _after is not None:
        raise ColumnNameInvalidError(
            'Only one of _before and _after can be specified.'
        )
    if _before is None and _after is None:
        rearranged = columns + rest_columns
    elif _before is not None:
        before_columns = select_columns(rest_columns, _before)
        cutpoint = min(rest_columns.index(bcol) for bcol in before_columns)
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    else: #_after
        after_columns = select_columns(rest_columns, _after)
        cutpoint = max(rest_columns.index(bcol) for bcol in after_columns) + 1
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    ret = _data[rearranged]
    if grouper is not None:
        return group_df(ret, grouper)
    return ret


@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def select(
        _data: DataFrameType,
        *columns: Union[StringOrIter, Inverted],
        **renamings: Mapping[str, str]
) -> DataFrameType:
    """Select (and optionally rename) variables in a data frame

    Args:
        *columns: The columns to select
        **renamings: The columns to rename and select in new => old column way.

    Returns:
        The dataframe with select columns
    """
    if isinstance(_data, DataFrameGroupBy):
        data = _data.obj
        groups = _data.grouper.names
    else:
        data = _data
        groups = []

    selected = select_columns(
        data.columns,
        *columns,
        *renamings.values()
    )
    selected = list_union(groups, selected)
    # old -> new
    new_names = {val: key for key, val in renamings.items() if val in selected}
    data = data[selected]
    if new_names:
        data = data.rename(columns=new_names)

    copy_flags(data, _data)
    if isinstance(_data, DataFrameGroupBy):
        return group_df(data, _data.grouper)
    return data


@register_verb(DataFrame, context=Context.SELECT)
def rowwise(_data: DataFrame, *columns: str) -> DataFrame:
    """Compute on a data frame a row-at-a-time

    Args:
        _data: The dataframe
        *columns:  Variables to be preserved when calling summarise().
            This is typically a set of variables whose combination
            uniquely identify each row.

    Returns:
        A row-wise data frame
    """
    check_column_uniqueness(_data)
    data = _data.copy()
    copy_flags(data, _data)
    if not columns:
        columns = True
    else:
        columns = select_columns(_data.columns, columns)
    data.flags.rowwise = columns
    return data

@rowwise.register(DataFrameGroupBy, context=Context.SELECT)
def _(_data: DataFrameGroupBy, *columns: str) -> DataFrame:
    if columns:
        raise ValueError(
            "Can't re-group when creating rowwise data."
        )
    data = _data.obj.copy()
    copy_flags(data, _data)
    data.flags.rowwise = _data.grouper.names
    return data

@register_verb(DataFrame, context=Context.PENDING)
def group_by(
        _data: DataFrame,
        *args: Any,
        _add: bool = False, # not working, since _data is not grouped
        _drop: Optional[bool] = None,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Takes an existing tbl and converts it into a grouped tbl where
    operations are performed "by group"

    Args:
        _data: The dataframe
        *args: variables or computations to group by.
        **kwargs: Extra variables to group the dataframe

    Return:
        A DataFrameGroupBy object
    """
    data = _data.copy()
    copy_flags(data, _data)
    mutated = _data >> mutate(*args, **kwargs, _keep='none')
    update_df(data, mutated)

    data.flags.groupby_drop = (
        group_by_drop_default(_data)
        if _drop is None else _drop
    )
    # requires pandas 1.2+
    # eariler versions have bugs with apply/transform
    # GH35889
    data.reset_index(drop=True, inplace=True)
    return group_df(data, mutated.columns.tolist(), drop=_drop)


@group_by.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *args: Any,
        _add: bool = False,
        _drop: Optional[bool] = None,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Group by on a DataFrameGroupBy object

    See group_by()

    Note:
        For _add argument, when FALSE, the default, group_by() will
        override existing groups. To add to the existing groups,
        use _add = TRUE.
    """
    mutated = _data >> mutate(*args, **kwargs, _keep='none')
    data = _data.obj.copy()
    update_df(data, mutated.obj)
    copy_flags(data, _data)

    if _add:
        columns = list_union(_data.grouper.names, mutated.obj.columns)
    else:
        columns = mutated.obj.columns.tolist()

    return group_df(data, columns, drop=_drop)

@register_verb(DataFrameGroupBy, context=Context.SELECT)
def ungroup(_data: DataFrameGroupBy, *cols: str) -> DataFrameType:
    """Ungroup a grouped dataframe

    Args:
        _data: The grouped dataframe
        *cols: Columns to remove from grouping

    Returns:
        The ungrouped dataframe or DataFrameGroupBy object with remaining
        grouping variables.
    """
    if not cols:
        return _data.obj

    gvars = _data.grouper.names
    for col in cols:
        if col not in gvars:
            raise ValueError(f'Not a grouping variable: {col!r}')
    new_vars = list_diff(gvars, cols)
    return group_df(_data, new_vars)

@ungroup.register(DataFrame, context=Context.EVAL)
def _(_data: DataFrame, *cols: str) -> DataFrame:
    if cols:
        raise ValueError(f'Dataframe is not grouped by {cols}')
    return _data

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


@register_verb(DataFrame, context=Context.PENDING)
def summarise(
        _data: DataFrame,
        *dfs: Union[DataFrame, Mapping[str, Iterable[Any]]],
        _groups: Optional[str] = None,
        **kwargs: Any
) -> DataFrameType:
    """Summarise each group to fewer rows

    See https://dplyr.tidyverse.org/reference/summarise.html

    Args:
        _groups: Grouping structure of the result.
            - "drop_last": dropping the last level of grouping.
            - "drop": All levels of grouping are dropped.
            - "keep": Same grouping structure as _data.
            - "rowwise": Each row is its own group.
        *dfs, **kwargs: Name-value pairs, where value is the summarized
            data for each group

    Returns:
        The summary dataframe.
    """
    check_column_uniqueness(
        _data,
        "Can't transform a data frame with duplicate names."
    )
    if getattr(_data.flags, 'rowwise', False):
        return _summarise_rowwise(
            _data,
            *dfs,
            _groups=_groups,
            **kwargs
        )

    context = Context.EVAL.value

    serieses = {}
    new_names = []
    for i, ser in enumerate(dfs):
        if isinstance(ser, Series):
            serieses[ser.name] = ser.values
        elif isinstance(ser, DataFrame):
            serieses.update(ser.to_dict('series'))
        elif isinstance(ser, dict):
            serieses.update(ser)
        else:
            serieses[f"V{i}"] = ser
            new_names.append(f"V{i}")

    serieses.update(kwargs)
    kwargs = serieses

    ret = None

    for key, val in kwargs.items():
        if val is None:
            continue
        if ret is None:
            val = evaluate_expr(val, _data, context)
            if key in new_names and isinstance(val, DataFrame):
                key = None
            ret = to_df(val, key)
            continue
        try:
            val = evaluate_expr(val, ret, context)
        except ColumnNotExistingError:
            val = evaluate_expr(val, _data, context)

        value = align_value(val, ret)
        df_assign_item(ret, key, value)

    if ret is None:
        ret = DataFrame(index=[0])

    copy_flags(ret, _data)
    ret.flags.rowwise = False

    if _groups == 'rowwise':
        ret.flags.rowwise = True

    return ret

@summarise.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *dfs: Union[DataFrame, Mapping[str, Iterable[Any]]],
        _groups: Optional[str] = None,
        **kwargs: Any
) -> DataFrameType:

    gsizes = []
    if _data.obj.shape[0] > 0:
        def apply_func(df):
            df.flags.grouper = _data.grouper
            ret = df >> summarise(*dfs, _groups=_groups, **kwargs)
            gsizes.append(0 if df.shape[0] == 0 else ret.shape[0])
            return ret

        applied = groupby_apply(_data, apply_func, groupdata=True)
    else: # 0-row dataframe
        # check cols in *dfs
        applied = DataFrame(
            columns=list_union(_data.grouper.names, kwargs.keys())
        )

    g_keys = _data.grouper.names
    if _groups is None:
        has_args = len(kwargs) > 0 or len(dfs) > 0
        all_ones = all(gsize <= 1 for gsize in gsizes)

        if applied.shape[0] <= 1 or all_ones or not has_args:
            _groups = 'drop_last'
            if len(g_keys) == 1 and summarise.inform:
                logger.info(
                    '`summarise()` ungrouping output '
                    '(override with `_groups` argument)'
                )
            elif summarise.inform:
                logger.info(
                    '`summarise()` has grouped output by '
                    '%s (override with `_groups` argument)',
                    g_keys[:-1]
                )
        else:
            _groups = 'keep'
            if summarise.inform:
                logger.info(
                    '`summarise()` has grouped output by %s. '
                    'You can override using the `_groups` argument.',
                    g_keys
                )

    copy_flags(applied, _data)

    if _groups == 'drop':
        return applied

    if _groups == 'drop_last':
        return group_df(applied, g_keys[:-1]) if g_keys[:-1] else applied

    if _groups == 'keep':
        # even keep the unexisting levels
        return group_df(applied, g_keys)

    # else:
    # todo: raise
    return applied

summarise.inform = True
summarize = summarise # pylint: disable=invalid-name


@register_verb(DataFrame, context=Context.EVAL)
def filter(
        _data: DataFrame,
        *conditions: Iterable[bool],
        _preserve: bool = False
) -> DataFrame:
    """Subset a data frame, retaining all rows that satisfy your conditions

    Args:
        condition, *conditions: Expressions that return logical values
        _preserve: Relevant when the .data input is grouped.
            If _preserve = FALSE (the default), the grouping structure
            is recalculated based on the resulting data, otherwise
            the grouping is kept as is.

    Returns:
        The subset dataframe
    """
    if _data.shape[0] == 0:
        return _data
    # check condition, conditions
    condition = numpy.array([True] * _data.shape[0])
    for cond in conditions:
        if is_scalar(cond):
            cond = numpy.array([cond] * _data.shape[0])
        condition = condition & cond
    try:
        condition = objectize(condition).values.flatten()
    except AttributeError:
        ...

    ret = _data[condition].reset_index(drop=True)
    copy_flags(ret, _data)
    return ret

@filter.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *conditions: Expression,
        _preserve: bool = False
) -> DataFrameGroupBy:
    """Filter on DataFrameGroupBy object"""
    if _data.obj.shape[0] > 0:
        def apply_func(df):
            df.flags.grouper = _data.grouper
            return df >> filter(*conditions)

        ret = groupby_apply(_data, apply_func, groupdata=True)
    else:
        ret = DataFrame(columns=_data.obj.columns)
    copy_flags(ret, _data)

    if _preserve:
        return group_df(ret, _data.grouper)
    return group_df(ret, _data.grouper.names)

# ------------------------------
# count

@register_verb(DataFrame, context=Context.EVAL)
def count(
        _data: DataFrame,
        *columns: Any,
        wt: Optional[NumericOrIter] = None,
        sort: bool = False,
        name: Optional[str] = None,
        _drop: Optional[bool] = None,
        **mutates: Any
) -> DataFrame:
    """Count observations by group

    See https://dplyr.tidyverse.org/reference/count.html

    Args:
        _data: The dataframe
        *columns: and
        **mutates: Variables to group by
        wt: Frequency weights. Can be None or a variable:
            If None (the default), counts the number of rows in each group.
            If a variable, computes sum(wt) for each group.
        sort: If TRUE, will show the largest groups at the top.
        name: The name of the new column in the output.

    Returns:
        DataFrame object with the count column
    """
    if _drop is None:
        _drop = group_by_drop_default(_data)

    mutated = _data >> mutate(*columns, **mutates, _keep='none')
    data = _data.copy()
    update_df(data, mutated)
    copy_flags(data, _data)

    columns = mutated.columns.tolist()
    if not columns:
        raise ValueError("No columns to count.")

    grouped = group_df(data, columns, drop=_drop)
    # check if name in columns
    if name is None:
        name = 'n'
        while name in columns:
            name += 'n'
        if name != 'n':
            logger.warning(
                'Storing counts in `%s`, as `n` already present in input. '
                'Use `name="new_name"` to pick a new name.',
                name
            )
    elif isinstance(name, str):
        columns = [col for col in columns if col != name]
    else:
        raise ValueError("`name` must be a single string.")

    if wt is None:
        count_frame = grouped[columns].grouper.size().to_frame(name=name)
    else:
        count_frame = Series(wt).groupby(
            grouped.grouper
        ).sum().to_frame(name=name)

    ret = count_frame.reset_index(level=columns)
    if sort:
        ret = ret.sort_values([name], ascending=[False])
    return ret

@count.register(DataFrameGroupBy, context=Context.PENDING)
def _(
        _data: DataFrameGroupBy,
        *columns: Any,
        wt: Optional[NumericOrIter] = None,
        sort: bool = False,
        name: Optional[str] = None,
        _drop: Optional[bool] = None,
        **mutates: Any
):
    if _drop is None:
        _drop = group_by_drop_default(_data)

    gkeys = _data.grouper.names

    def apply_func(df):
        if df.shape[0] == 0:
            return None
        return df >> count(
            df[gkeys],
            *columns,
            wt=wt,
            sort=sort,
            name=name,
            _drop=True,
            **mutates
        )

    applied = groupby_apply(_data, apply_func)# index reset

    if not _drop:
        if len(gkeys) > 1 or not is_categorical(_data.obj[gkeys[0]]):
            logger.warning(
                'Currently, _drop=False of count on grouped dataframe '
                'only works when dataframe is grouped by a single '
                'categorical column.'
            )
        else:
            applied = applied.set_index(gkeys).reindex(
                _data.obj[gkeys[0]].cat.categories,
                fill_value=0
            ).reset_index(level=gkeys)

    # not dropping anything
    return group_df(applied, gkeys)


@register_verb(DataFrameGroupBy, context=Context.PENDING)
def tally(
        _data: DataFrameGroupBy,
        wt: Optional[NumericOrIter] = None,
        sort: bool = False,
        name: Optional[str] = None
) -> DataFrame:
    """A ower-level function for count that assumes you've done the grouping

    See count()
    """
    ret = _data >> count(wt=wt, sort=sort, name=name)
    return ret.obj if isinstance(ret, DataFrameGroupBy) else ret

@tally.register(DataFrame, context=Context.EVAL)
def _(
        _data: DataFrame,
        wt: Optional[NumericOrIter] = None,
        sort: bool = False, # pylint: disable=unused-argument
        name: Optional[str] = None
) -> DataFrame:
    """tally for DataFrame object"""
    name = name or 'n'
    if wt is None:
        wt = _data.shape[0]
    else:
        wt = wt.sum()
    return DataFrame({name: [wt]})

@register_verb((DataFrame, DataFrameGroupBy), context=Context.PENDING)
def add_count(
        _data: DataFrameType,
        *columns: Any,
        wt: Optional[str] = None,
        sort: bool = False,
        name: str = 'n',
        **mutates: Any
) -> DataFrameType:
    """Equivalents to count() but use mutate() instead of summarise()

    See count().
    """
    count_frame = count(_data, *columns, wt=wt, sort=sort, name=name, **mutates)
    ret = objectize(_data).merge(
        count_frame,
        on=count_frame.columns.to_list()[:-1]
    )

    if sort:
        ret = ret.sort_values([name], ascending=[False])

    copy_flags(ret, _data)
    if isinstance(_data, DataFrameGroupBy):
        return group_df(ret, _data.grouper)
    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.PENDING)
def add_tally(
        _data: DataFrameType,
        wt: Optional[str] = None,
        sort: bool = False,
        name: str = 'n'
) -> DataFrameType:
    """Equivalents to tally() but use mutate() instead of summarise()

    See count().
    """
    tally_frame = tally(_data, wt=wt, sort=False, name=name)

    ret = objectize(_data).assign(**{
        name: tally_frame.values.flatten()[0]
    })

    if sort:
        ret = ret.sort_values([name], ascending=[False])

    copy_flags(ret, _data)
    if isinstance(_data, DataFrameGroupBy):
        return group_df(ret, _data.grouper)

    return ret

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

@register_verb(
    (DataFrame, DataFrameGroupBy),
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def semi_join(
        x: DataFrameType,
        y: DataFrameType,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False
) -> DataFrameType:
    """Returns all rows from x with a match in y.

    See inner_join()
    """
    xobj = objectize(x)
    y = objectize(y)
    ret = pandas.merge(
        xobj, y,
        on=by,
        how='left',
        copy=copy,
        suffixes=['', '_y'],
        indicator=True
    )
    ret = ret.loc[ret._merge == 'both', xobj.columns.tolist()]

    copy_flags(ret, x)
    if isinstance(x, DataFrameGroupBy):
        return group_df(ret, x.grouper.names)

    return ret

@register_verb(
    (DataFrame, DataFrameGroupBy),
    context=Context.EVAL,
    extra_contexts={'by': Context.SELECT}
)
def anti_join(
        x: DataFrameType,
        y: DataFrameType,
        by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
        copy: bool = False
) -> DataFrameType:
    """Returns all rows from x without a match in y.

    See inner_join()
    """
    xobj = objectize(x)
    y = objectize(y)
    ret = pandas.merge(
        xobj, y,
        on=by,
        how='left',
        copy=copy,
        suffixes=['', '_y'],
        indicator=True
    )
    ret = ret.loc[ret._merge != 'both', xobj.columns.tolist()]

    copy_flags(ret, x)
    if isinstance(x, DataFrameGroupBy):
        return group_df(ret, x.grouper.names)

    return ret
