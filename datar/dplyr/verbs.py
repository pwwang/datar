"""Verbs ported from R-dplyr"""
import numpy
import pandas
from pandas.core.groupby import grouper
from pandas.core.indexes.multi import MultiIndex
from pipda.utils import evaluate_args
from datar.core.types import DataFrameType, SeriesLikeType, StringOrIter, is_scalar
from typing import Any, Callable, Iterable, List, Mapping, Optional, Union

from pandas import DataFrame, Series
from pandas.core.groupby.generic import DataFrameGroupBy, SeriesGroupBy
from pipda import register_verb, Context, evaluate_expr

from ..core.middlewares import (
    Across, CAcross, Collection,
    DescSeries, IfCross, Inverted, RowwiseDataFrame
)
from ..core.contexts import ContextEvalWithUsedRefs, ContextSelectSlice
from ..core.exceptions import ColumnNameInvalidError
from ..core.utils import (
    align_value, copy_df, df_assign_item, expand_slice, get_n_from_prop,
    list_diff, list_intersect, list_union,
    objectize, select_columns, to_df
)

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def arrange(
        _data: Union[DataFrame, DataFrameGroupBy],
        *series: Union[Series, SeriesGroupBy, Across],
        _by_group: bool = False
) -> Union[DataFrame, DataFrameGroupBy]:
    """orders the rows of a data frame by the values of selected columns.

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
    data = objectize(_data)
    sorting_df = (
        data.index.to_frame(name='__index__').drop(columns=['__index__'])
    )
    desc_cols = set()
    acrosses = []
    kwargs = {}
    for ser in series:
        if isinstance(ser, Across):
            desc_cols |= ser.desc_cols()
            if ser.fns:
                acrosses.append(ser)
            else:
                for col in ser.cols:
                    kwargs[col] = data[col].values
        else:
            ser = objectize(ser)
            if isinstance(ser, DescSeries):
                desc_cols.add(ser.name)
            kwargs[ser.name] = ser.values

    sorting_df = mutate(sorting_df, *acrosses, **kwargs)

    by = sorting_df.columns.to_list()
    if isinstance(_data, DataFrameGroupBy):
        for key in _data.grouper.names:
            if key not in sorting_df:
                sorting_df[key] = _data[key].obj.values
        if _by_group:
            by = list_union(_data.grouper.names, by)

    ascending = [col not in desc_cols for col in by]
    sorting_df.sort_values(by=by, ascending=ascending, inplace=True)
    data = data.loc[sorting_df.index, :]

    if isinstance(_data, DataFrameGroupBy):
        return data.groupby(_data.grouper.names, dropna=False)

    return data


@register_verb((DataFrame, DataFrameGroupBy), context=None)
def mutate(
        _data: DataFrame,
        *acrosses: Across,
        _keep: str = 'all',
        _before: Optional[str] = None,
        _after: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:
    """adds new variables and preserves existing ones

    Args:
        _data: A data frame
        *acrosses: Values from across function
        _keep: allows you to control which columns from _data are retained
            in the output:
            - "all", the default, retains all variables.
            - "used" keeps any variables used to make new variables;
                it's useful for checking your work as it displays inputs and
                outputs side-by-side.
            - "unused" keeps only existing variables not used to make new
                variables.
            - "none", only keeps grouping keys (like transmute()).
        _before, _after: Optionally, control where new columns should appear
            (the default is to add to the right hand side).
            See relocate() for more details.
        **kwargs: Name-value pairs. The name gives the name of the column
            in the output. The value can be:
            - A vector of length 1, which will be recycled to the correct
                length.
            - A vector the same length as the current group (or the whole
                data frame if ungrouped).
            - None to remove the column

    Returns:
        An object of the same type as .data. The output has the following
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
    context = ContextEvalWithUsedRefs()
    if _before is not None:
        _before = evaluate_expr(_before, _data, Context.SELECT)
    if _after is not None:
        _after = evaluate_expr(_after, _data, Context.SELECT)

    across = {} # no need OrderedDict in python3.7+ anymore
    for acrs in acrosses:
        acrs = evaluate_expr(acrs, _data, context)
        if isinstance(acrs, Across):
            across.update(acrs.evaluate(context))
        else:
            across.update(acrs)

    across.update(kwargs)
    kwargs = across

    if isinstance(_data, RowwiseDataFrame):
        data = RowwiseDataFrame(_data, rowwise=_data.rowwise)
    else:
        data = copy_df(_data)

    for key, val in kwargs.items():
        if val is None:
            data.drop(columns=[key], inplace=True)
            continue
        val = evaluate_expr(val, data, context)

        if isinstance(val, CAcross):
            val.names = key
        if isinstance(val, Across):
            val = DataFrame(val.evaluate(context, data))

        value = align_value(val, data)
        df_assign_item(data, key, value)

    outcols = list(kwargs)
    # do the relocate first
    if _before is not None or _after is not None:
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
    if (isinstance(_data, RowwiseDataFrame) and
            not isinstance(data, RowwiseDataFrame)):
        return RowwiseDataFrame(data, rowwise=_data.rowwise)

    return data

# Forward pipda.Expression for mutate to evaluate
@register_verb((DataFrame, DataFrameGroupBy), context=None)
def transmutate(
        _data: DataFrame,
        *acrosses: Across,
        _before: Optional[str] = None,
        _after: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:
    """Mutate with _keep='none'

    See mutate().
    """
    return mutate(
        _data,
        *acrosses,
        _keep='none',
        _before=_before,
        _after=_after,
        **kwargs
    )

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def relocate(
        _data: Union[DataFrame, DataFrameGroupBy],
        column: str,
        *columns: str,
        _before: Optional[Union[int, str]] = None,
        _after: Optional[Union[int, str]] = None,
) -> Union[DataFrame, DataFrameGroupBy]:
    """change column positions

    Args:
        _data: A data frame
        column, *columns: Columns to move
        _before, _after: Destination. Supplying neither will move columns to
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
        return ret.groupby(grouper, dropna=False)
    return ret


@register_verb((DataFrame, DataFrameGroupBy))
def select(
        _data: DataFrameType,
        *columns: Union[StringOrIter, Inverted],
        **renamings: Mapping[str, str]
) -> DataFrame:
    """Select (and optionally rename) variables in a data frame

    Args:
        *columns: The columns to select
        **renamings: The columns to rename and select in new => old column way.

    Returns:
        The dataframe with select columns
    """
    selected = select_columns(
        _data.columns,
        *columns,
        *renamings.values()
    )
    # old -> new
    new_names = {val: key for key, val in renamings.items() if val in selected}
    if new_names:
        return _data[selected].rename(columns=new_names)
    return _data[selected]


@register_verb((DataFrame, DataFrameGroupBy))
def rowwise(_data: DataFrameType, *columns: str) -> RowwiseDataFrame:
    """Compute on a data frame a row-at-a-time

    Note:
        If the dataframe is grouped, the group information will be lost

    Args:
        _data: The dataframe
        *columns:  Variables to be preserved when calling summarise().
            This is typically a set of variables whose combination
            uniquely identify each row.

    Returns:
        A row-wise data frame with class RowwiseDataFrame
    """
    _data = objectize(_data)
    columns = select_columns(_data.columns, columns)
    return RowwiseDataFrame(_data, rowwise=columns)

@register_verb(DataFrame, context=None)
def group_by(
        _data: DataFrame,
        *columns: str,
        _add: bool = False, # not working, since _data is not grouped
        **kwargs: Any
) -> DataFrameGroupBy:
    """Takes an existing tbl and converts it into a grouped tbl where
    operations are performed "by group"

    Args:
        _data: The dataframe
        *columns: variables or computations to group by.
        **kwargs: Extra variables to group the dataframe

    Return:
        A DataFrameGroupBy object
    """
    if kwargs:
        _data = mutate(_data, **kwargs)

    columns = evaluate_args(columns, _data, Context.SELECT)
    columns = select_columns(_data.columns, *columns, *kwargs.keys())
    # requires pandas 1.2+
    # eariler versions have bugs with apply/transform
    # GH35889
    return _data.groupby(columns, dropna=False)


@group_by.register(DataFrameGroupBy)
def _(
    _data: DataFrameGroupBy,
    *columns: str,
    _add: bool = False,
    **kwargs: Any
) -> DataFrameGroupBy:
    """Group by on a DataFrameGroupBy object

    See group_by()

    Note:
        For _add argument, when FALSE, the default, group_by() will
        override existing groups. To add to the existing groups,
        use _add = TRUE.
    """
    if kwargs:
        _data = mutate(_data, **kwargs)

    columns = evaluate_args(columns, _data, Context.SELECT)
    columns = select_columns(_data.obj.columns, *columns, *kwargs.keys())
    if _add:
        groups = Collection(*_data.grouper.names) + columns
        return _data.obj.groupby(groups, dropna=False)
    return _data.obj.groupby(columns, dropna=False)


@register_verb(DataFrameGroupBy)
def ungroup(_data: DataFrameGroupBy) -> DataFrame:
    """Ungroup a grouped dataframe

    Args:
        _data: The grouped dataframe

    Returns:
        The ungrouped dataframe
    """
    return _data.obj

# ------------------------------
# group data

@register_verb((DataFrame, DataFrameGroupBy))
def group_keys(
        _data: Union[DataFrame, DataFrameGroupBy],
        *cols: str,
        **kwargs: Any,
) -> DataFrame:
    """Get the group keys as a dataframe"""
    if not isinstance(_data, DataFrameGroupBy):
        _data = group_by(_data, *cols, **kwargs)
    group_levels = list(_data.groups.keys())
    return DataFrame(group_levels, columns=_data.grouper.names)

@register_verb(DataFrameGroupBy)
def group_rows(_data: DataFrameGroupBy) -> List[str]:
    """Returns the rows which each group contains"""
    return _data.grouper.groups

@register_verb(DataFrameGroupBy)
def group_vars(_data: DataFrameGroupBy) -> List[str]:
    """gives names of grouping variables as character vector"""
    return _data.grouper.names

group_cols = group_vars

@register_verb((DataFrame, DataFrameGroupBy))
def group_map(
        _data: Union[DataFrame, DataFrameGroupBy],
        func: Callable[[DataFrame], Any]
) -> List[Any]:
    """Map function to data in each group, returns a list"""
    if isinstance(_data, DataFrame):
        return func(_data)
    return [
        func(_data.obj.loc[index]) for index in _data.grouper.groups.values()
    ]

@register_verb((DataFrame, DataFrameGroupBy))
def group_modify(
        _data: Union[DataFrame, DataFrameGroupBy],
        func: Callable[[DataFrame], DataFrame]
) -> DataFrame:
    """Modify data in each group with func, returns a dataframe"""
    if isinstance(_data, DataFrame):
        return func(_data)
    return _data.apply(func).reset_index(drop=True, level=0)

@register_verb((DataFrame, DataFrameGroupBy))
def group_walk(
        _data: Union[DataFrame, DataFrameGroupBy],
        func: Callable[[DataFrame], None]
) -> None:
    """Walk along data in each groups, but don't return anything"""
    if isinstance(_data, DataFrame):
        func(_data)
    _data.apply(func)

@register_verb(DataFrameGroupBy)
def group_trim(
        _data: DataFrameGroupBy
) -> DataFrameGroupBy:
    """Trim the unused group levels"""
    return _data.obj.groupby(_data.grouper.names)

@register_verb((DataFrame, DataFrameGroupBy))
def group_split(
        _data: Union[DataFrame, DataFrameGroupBy],
        *cols: str,
        _keep: bool = False,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Get a list of data in each group"""
    if isinstance(_data, RowwiseDataFrame):
        _data = objectize(_data)
        return [_data.iloc[[i], :] for i in range(_data.shape[0])]

    if isinstance(_data, DataFrameGroupBy):
        return [
            _data.obj.loc[index] for index in _data.grouper.groups.values()
        ]

    _data = group_by(_data, *cols, **kwargs)
    return group_split(_data)

@register_verb((DataFrame, DataFrameGroupBy), context=Context.UNSET)
def with_groups(
        _data: Union[DataFrame, DataFrameGroupBy],
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

@register_verb((DataFrame, DataFrameGroupBy), context=Context.MIXED)
def summarise(
        _data: Union[DataFrame, DataFrameGroupBy],
        *acrosses: Across,
        _groups: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:
    """Summarise each group to fewer rows

    See: https://dplyr.tidyverse.org/reference/summarise.html

    Args:
        _groups: Grouping structure of the result.
            - "drop_last": dropping the last level of grouping.
            - "drop": All levels of grouping are dropped.
            - "keep": Same grouping structure as _data.
            - "rowwise": Each row is its own group.
        *acrosses, **kwargs: Name-value pairs, where value is the summarized
            data for each group

    Returns:
        The summary dataframe.
    """
    across = {} # no need OrderedDict in python3.7+ anymore
    for acrs in acrosses:
        across.update(
            acrs.evaluate(Context.EVAL, _data)
            if isinstance(acrs, Across)
            else acrs
        )

    across.update(kwargs)
    kwargs = across

    ret = None
    if isinstance(_data, RowwiseDataFrame) and _data.rowwise is not True:
        ret = _data.loc[:, _data.rowwise]

    for key, val in kwargs.items():
        if isinstance(val, CAcross):
            val.names = key
        if isinstance(val, Across):
            val = DataFrame(val.evaluate(Context.EVAL, _data))

        if ret is None:
            ret = to_df(val, key)
        # if isinstance(val, Series) and val.index.name == ret.index.name:
        #     # in case val has more rows than ret, ie. quantile
        #     # we expand ret
        #     ret =  ret.loc[val.index, :]
        #     ret[key] = val
        else:
            ret[key] = align_value(val, ret)

    if _groups == 'rowwise':
        return RowwiseDataFrame(ret)

    if not isinstance(_data, DataFrameGroupBy):
        return ret

    if _groups is None:
        if ret.shape[0] == 1:
            _groups = 'drop_last'
        elif isinstance(ret.index, MultiIndex):
            _groups = 'drop_last'

    ret.reset_index(inplace=True)
    group_keys = _data.grouper.names
    if _groups == 'drop_last':
        return ret.groupby(group_keys[:-1]) if group_keys[:-1] else ret

    if _groups == 'keep':
        return ret.groupby(_data.grouper) # even keep the unexisting levels

    # else:
    # todo: raise
    return ret

summarize = summarise


@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def filter(
        _data: DataFrameType,
        condition,
        *conditions,
        _preserve=False
) -> DataFrameType:
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
    if isinstance(condition, IfCross):
        condition = condition.evaluate(Context.EVAL, _data)

    # check condition, conditions
    for cond in conditions:
        if isinstance(cond, IfCross):
            cond = cond.evaluate(Context.EVAL, _data)
        condition = condition & cond
    try:
        condition = objectize(condition).values.flatten()
    except AttributeError:
        ...

    ret = objectize(_data)[condition]
    if isinstance(_data, DataFrameGroupBy):
        grouper = _data.grouper if _preserve else _data.grouper.names
        return ret.groupby(grouper, dropna=True)
    return ret

# ------------------------------
# count

@register_verb((DataFrame, DataFrameGroupBy), context=None)
def count(
        _data: Union[DataFrame, DataFrameGroupBy],
        *columns: Any,
        wt: Optional[str] = None,
        sort: bool = False,
        name: str = 'n',
        **mutates: Any
) -> DataFrame:
    """Count observations by group

    See: https://dplyr.tidyverse.org/reference/count.html

    Args:
        _data: The dataframe
        *columns, **mutates: Variables to group by
        wt: Frequency weights. Can be None or a variable:
            If None (the default), counts the number of rows in each group.
            If a variable, computes sum(wt) for each group.
        sort: If TRUE, will show the largest groups at the top.
        name: The name of the new column in the output.

    Returns:
        DataFrame object with the count column
    """
    _data = objectize(_data)
    columns = evaluate_args(columns, _data, Context.SELECT)
    columns = select_columns(_data.columns, *columns)

    wt = evaluate_expr(wt, _data, Context.SELECT)
    _data = mutate(_data, **mutates)

    columns = columns + list(mutates)
    grouped = _data.groupby(columns, dropna=False)

    if not wt:
        count_frame = grouped[columns].size().to_frame(name=name)
    else:
        count_frame = grouped[wt].sum().to_frame(name=name)

    ret = count_frame.reset_index(level=columns)
    if sort:
        ret = ret.sort_values([name], ascending=[False])
    return ret


@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def tally(
        _data: Union[DataFrame, DataFrameGroupBy],
        wt: str = None,
        sort: bool = False,
        name: str = 'n'
) -> DataFrame:
    """A ower-level function for count that assumes you've done the grouping

    See count()
    """
    if isinstance(_data, DataFrameGroupBy):
        return count(_data, *_data.grouper.names, wt=wt, sort=sort, name=name)

    return DataFrame({
        name: [_data.shape[0] if wt is None else _data[wt].sum()]
    })

@register_verb((DataFrame, DataFrameGroupBy), context=None)
def add_count(
        _data: Union[DataFrame, DataFrameGroupBy],
        *columns: Any,
        wt: Optional[str] = None,
        sort: bool = False,
        name: str = 'n',
        **mutates: Any
) -> Union[DataFrame, DataFrameGroupBy]:
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

    if isinstance(_data, DataFrameGroupBy):
        return ret.groupby(_data.grouper.names, dropna=False)
    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def add_tally(
        _data: Union[DataFrame, DataFrameGroupBy],
        wt: str = None,
        sort: bool = False,
        name: str = 'n'
) -> Union[DataFrame, DataFrameGroupBy]:
    """Equivalents to tally() but use mutate() instead of summarise()

    See count().
    """
    tally_frame = tally(_data, wt=wt, sort=False, name=name)

    ret = objectize(_data).assign(**{
        name: tally_frame.values.flatten()[0]
    })

    if sort:
        ret = ret.sort_values([name], ascending=[False])

    if isinstance(_data, DataFrameGroupBy):
        return ret.groupby(_data.grouper.names, dropna=False)

    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.MIXED)
def distinct(
        _data: Union[DataFrame, DataFrameGroupBy],
        *columns: Any,
        _keep_all: bool = False,
        **mutates: Any
) -> Union[DataFrame, DataFrameGroupBy]:
    """Select only unique/distinct rows from a data frame.

    Args:
        _data: The dataframe
        *columns, **mutates: Optional variables to use when determining
            uniqueness.
        _keep_all: If TRUE, keep all variables in _data

    Returns:
        A dataframe without duplicated rows in _data
    """
    data = objectize(_data)

    all_columns = data.columns
    columns = select_columns(all_columns, *columns)
    if isinstance(_data, DataFrameGroupBy):
        columns = list_union(_data.grouper.names, columns)

    data = mutate(data, **mutates)
    columns = columns + list(mutates)

    if not columns:
        columns = all_columns

    uniq_frame = data.drop_duplicates(columns, ignore_index=True)
    ret = uniq_frame if _keep_all else uniq_frame[columns]
    if isinstance(_data, DataFrameGroupBy):
        return ret.groupby(_data.grouper.names, dropna=False)
    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def pull(
        _data: Union[DataFrame, DataFrameGroupBy],
        var: Union[int, str] = -1,
        name: Optional[StringOrIter] = None,
        to_list: bool = False
) -> SeriesLikeType:
    """Pull a series from a dataframe

    Args:
        _data: The dataframe
        var: The column to pull
        name: If specified, a zip object will be return with the name-value
            pairs. It can be a column name or a list of strs with the same
            length as the series
        to_list: Whether return a python list or keep it numpy.ndarray.

    Returns:
        The series data.
    """
    _data = objectize(_data)
    if isinstance(var, int):
        var = _data.columns[var]

    if name is not None and is_scalar(name):
        return zip(_data[name].values, _data[var].values)
    if name is not None:
        return zip(name, _data[var].values)

    if to_list:
        return _data[var].values.tolist()
    return _data[var].values

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
    return _data.rename(columns={val: key for key, val in kwargs.items()})

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
        _preserve: Relevant when the .data input is grouped.
            If _preserve = FALSE (the default), the grouping structure is
            recalculated based on the resulting data,
            otherwise the grouping is kept as is.

    Returns:
        The sliced dataframe
    """
    rows = expand_slice(rows, _data.shape[0])
    return _data.iloc[rows, :]

@slice.register(DataFrameGroupBy)
def _(
        _data: DataFrameGroupBy,
        rows: Any,
        _preserve: bool = False
) -> DataFrameGroupBy:
    """Slice on grouped datafram"""
    grouper = _data.grouper
    _data = objectize(_data)
    rows = expand_slice(rows, _data.shape[0])
    ret = _data.iloc[rows, :]
    if not _preserve:
        return _data.groupby(grouper.names, dropna=False)
    return _data.groupby(grouper, dropna=False)

@register_verb(DataFrame)
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

@slice_head.register(DataFrameGroupBy)
def _(
        _data: DataFrameGroupBy,
        n: Optional[Union[int, Iterable[int]]] = None,
        prop: Optional[Union[float, Iterable[float]]] = None
) -> DataFrame:
    """slice_head on grouped dataframe"""
    # any other better way?
    total = _data.size().to_frame(name='size')
    total['n'] = n
    total['prop'] = prop
    indexes = total.apply(
        lambda row: _data.groups[row.name][
            :get_n_from_prop(row.size, row.n, row.prop)
        ],
        axis=1
    )
    indexes = numpy.concatenate(indexes.values)
    return _data.obj.iloc[indexes, :].groupby(_data.grouper, dropna=False)

@register_verb(DataFrame)
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

@slice_tail.register(DataFrameGroupBy)
def _(
        _data: DataFrameGroupBy,
        n: Optional[Union[int, Iterable[int]]] = None,
        prop: Optional[Union[float, Iterable[float]]] = None
) -> DataFrame:
    """slice_tail on grouped dataframe"""
    # any other better way?
    total = _data.size().to_frame(name='size')
    total['n'] = n
    total['prop'] = prop
    indexes = total.apply(
        lambda row: _data.groups[row.name][
            -get_n_from_prop(row.size, row.n, row.prop):
        ],
        axis=1
    )
    indexes = numpy.concatenate(indexes.values)
    return _data.obj.iloc[indexes, :].groupby(_data.grouper, dropna=False)

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

# todo: slice_min.register(DataFrameGroupBy)

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

# todo: slice_max.register(DataFrameGroupBy)

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

# todo: slice_sample.register(DataFrameGroupBy)


# Two table verbs
# ---------------

@register_verb(DataFrame)
def bind_rows(
        _data: DataFrame,
        *datas: DataFrame
) -> DataFrame:
    """Bind rows of give dataframes

    Args:
        _data, *datas: Dataframes to combine

    Returns:
        The combined dataframe
    """
    return pandas.concat([_data, *datas])

@register_verb(DataFrame)
def bind_cols(
        _data: DataFrame,
        *datas: DataFrame
) -> DataFrame:
    """Bind columns of give dataframes

    Args:
        _data, *datas: Dataframes to combine

    Returns:
        The combined dataframe
    """
    return pandas.concat([_data, *datas], axis=1)

@register_verb(DataFrame)
def intersect(
        _data: DataFrame,
        *datas: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Intersect of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of intersect of input dataframes
    """
    if not on:
        on = _data.columns.to_list()

    return pandas.merge(_data, *datas, on=on, how='inner') >> distinct(*on)

@register_verb(DataFrame)
def union(
        _data: DataFrame,
        *datas: DataFrame,
        on: Optional[StringOrIter] = None
) -> DataFrame:
    """Union of two dataframes

    Args:
        _data, *datas: Dataframes to perform operations
        on: The columns to the dataframes to perform operations on

    Returns:
        The dataframe of union of input dataframes
    """
    if not on:
        on = _data.columns.to_list()

    return pandas.merge(_data, *datas, on=on, how='outer') >> distinct(*on)

@register_verb(DataFrame)
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
        lambda x : x['_merge']=='left_only'
    ].drop(columns=['_merge']) >> distinct(*on)

@register_verb(DataFrame)
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

@register_verb(DataFrame)
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

@register_verb(DataFrame)
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
    if isinstance(by, dict):
        right_on=list(by.values())
        ret = pandas.merge(
            x, y,
            left_on=list(by.keys()),
            right_on=right_on,
            how='inner',
            copy=copy,
            suffixes=suffix
        )
        if not keep:
            return ret.drop(columns=right_on)
        return ret
    return pandas.merge(x, y, on=by, how='inner', copy=copy, suffixes=suffix)

@register_verb(DataFrame)
def left_join(
    x: DataFrame,
    y: DataFrame,
    by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in x.

    See inner_join()
    """
    if isinstance(by, dict):
        right_on=list(by.values())
        ret = pandas.merge(
            x, y,
            left_on=list(by.keys()),
            right_on=right_on,
            how='left',
            copy=copy,
            suffixes=suffix
        )
        if not keep:
            return ret.drop(columns=right_on)
        return ret
    return pandas.merge(x, y, on=by, how='left', copy=copy, suffixes=suffix)

@register_verb(DataFrame)
def right_join(
    x: DataFrame,
    y: DataFrame,
    by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in y.

    See inner_join()
    """
    if isinstance(by, dict):
        right_on=list(by.values())
        ret = pandas.merge(
            x, y,
            left_on=list(by.keys()),
            right_on=right_on,
            how='right',
            copy=copy,
            suffixes=suffix
        )
        if not keep:
            return ret.drop(columns=right_on)
        return ret
    return pandas.merge(x, y, on=by, how='right', copy=copy, suffixes=suffix)

@register_verb(DataFrame)
def full_join(
    x: DataFrame,
    y: DataFrame,
    by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
    copy: bool = False,
    suffix: Iterable[str] = ("_x", "_y"),
    keep: bool = False
) -> DataFrame:
    """Mutating joins including all rows in x or y.

    See inner_join()
    """
    if isinstance(by, dict):
        right_on=list(by.values())
        ret = pandas.merge(
            x, y,
            left_on=list(by.keys()),
            right_on=right_on,
            how='outer',
            copy=copy,
            suffixes=suffix
        )
        if not keep:
            return ret.drop(columns=right_on)
        return ret
    return pandas.merge(x, y, on=by, how='outer', copy=copy, suffixes=suffix)

@register_verb(DataFrame)
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

    y_matched = x.apply(lambda row: get_nested_df(row), axis=1)
    y_name = getattr(y, '__dfname__', None)
    if y_name:
        y_matched = y_matched.to_frame(name=y_name)
    return pandas.concat([x, y_matched], axis=1)

@register_verb(DataFrame)
def semi_join(
    x: DataFrame,
    y: DataFrame,
    by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
    copy: bool = False
) -> DataFrame:
    """Returns all rows from x with a match in y.

    See inner_join()
    """
    ret = pandas.merge(x, y, on=by, how='left', copy=copy, indicator=True)
    return ret[ret._merge == 'both'].loc[:, x.columns.tolist()]

@register_verb(DataFrame)
def anti_join(
    x: DataFrame,
    y: DataFrame,
    by: Optional[Union[StringOrIter, Mapping[str, str]]] = None,
    copy: bool = False
) -> DataFrame:
    """Returns all rows from x without a match in y.

    See inner_join()
    """
    ret = pandas.merge(x, y, on=by, how='left', copy=copy, indicator=True)
    return ret[ret._merge != 'both'].loc[:, x.columns.tolist()]
