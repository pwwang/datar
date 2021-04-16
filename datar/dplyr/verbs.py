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




# ------------------------------
# count


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


# Two table verbs
# ---------------


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
