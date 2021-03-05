"""Verbs and functions in dplyr"""
from typing import Any, Optional, Union

from pandas import DataFrame, Series
from pandas.core.groupby.generic import DataFrameGroupBy, SeriesGroupBy
from pipda import register_verb, Context, evaluate_expr

from .core.middlewares import Across, CAcross, DescSeries, RowwiseDataFrame
from .core.contexts import ContextEvalWithUsedRefs
from .core.exceptions import ColumnNameInvalidError
from .core.utils import (
    align_value, copy_df, df_assign_item, list_diff, list_union,
    objectize, select_columns
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
            - Existing columns will be preserved according to the .keep
                argument. New columns will be placed according to the
                .before and .after arguments. If .keep = "none"
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
        before_columns = select_columns(all_columns, _before)
        cutpoint = min(rest_columns.index(bcol) for bcol in before_columns)
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    else: #_after
        after_columns = select_columns(all_columns, _after)
        cutpoint = max(rest_columns.index(bcol) for bcol in after_columns) + 1
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    ret = _data[rearranged]
    if grouper is not None:
        return ret.groupby(grouper, dropna=False)
    return ret
