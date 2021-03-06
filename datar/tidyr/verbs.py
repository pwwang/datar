"""Verbs from R-tidyr"""
from typing import Any, Callable, Mapping, Optional, Type, Union

import numpy
import pandas
from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from pipda import register_verb, Context

from ..core.utils import objectize, select_columns, list_diff
from ..core.types import DataFrameType, IntOrIter, StringOrIter, is_scalar

@register_verb(DataFrame, context=Context.SELECT)
def pivot_longer(
        _data: DataFrame,
        cols: StringOrIter,
        names_to: StringOrIter = "name",
        names_prefix: Optional[str] = None,
        names_sep: Optional[str] = None,
        names_pattern: Optional[str] = None,
        names_ptypes: Optional[Mapping[str, Type]] = None,
        names_transform: Optional[Mapping[str, Callable]] = None,
        # names_repair="check_unique", # todo
        values_to: str = "value",
        values_drop_na: bool = False,
        values_ptypes: Optional[Mapping[str, Type]] = None,
        values_transform: Optional[Mapping[str, Callable]] = None
):
    """"lengthens" data, increasing the number of rows and
    decreasing the number of columns.

    Args:
        _data: A data frame to pivot.
        cols: Columns to pivot into longer format.
        names_to: A string specifying the name of the column to create from
            the data stored in the column names of data.
            Can be a character vector, creating multiple columns, if names_sep
            or names_pattern is provided. In this case, there are two special
            values you can take advantage of:
            - None will discard that component of the name.
            - .value indicates that component of the name defines the name of
                the column containing the cell values, overriding values_to.
        names_prefix: A regular expression used to remove matching text from
            the start of each variable name.
        names_sep, names_pattern: If names_to contains multiple values,
            these arguments control how the column name is broken up.
            names_sep takes the same specification as separate(), and
            can either be a numeric vector (specifying positions to break on),
            or a single string (specifying a regular expression to split on).
        names_pattern: takes the same specification as extract(),
            a regular expression containing matching groups (()).
        names_ptypes, values_ptypes: A list of column name-prototype pairs.
            A prototype (or ptype for short) is a zero-length vector
            (like integer() or numeric()) that defines the type, class, and
            attributes of a vector. Use these arguments if you want to confirm
            that the created columns are the types that you expect.
            Note that if you want to change (instead of confirm) the types
            of specific columns, you should use names_transform or
            values_transform instead.
        names_transform, values_transform: A list of column name-function pairs.
            Use these arguments if you need to change the types of
            specific columns. For example,
            names_transform = dict(week = as.integer) would convert a
            character variable called week to an integer.
            If not specified, the type of the columns generated from names_to
            will be character, and the type of the variables generated from
            values_to will be the common type of the input columns used to
            generate them.
        names_repair: Not supported yet.
        values_to: A string specifying the name of the column to create from
            the data stored in cell values. If names_to is a character
            containing the special .value sentinel, this value will be ignored,
            and the name of the value column will be derived from part of
            the existing column names.
        values_drop_na: If TRUE, will drop rows that contain only NAs in
            the value_to column. This effectively converts explicit missing
            values to implicit missing values, and should generally be used
            only when missing values in data were created by its structure.

    Returns:
        The pivoted dataframe.
    """
    columns = select_columns(_data.columns, cols)
    id_columns = list_diff(_data.columns, columns)
    var_name = '__tmp_names_to__' if names_pattern or names_sep else names_to
    ret = _data.melt(
        id_vars=id_columns,
        value_vars=columns,
        var_name=var_name,
        value_name=values_to,
    )

    if names_pattern:
        ret[names_to] = ret['__tmp_names_to__'].str.extract(names_pattern)
        ret.drop(['__tmp_names_to__'], axis=1, inplace=True)

    if names_prefix:
        ret[names_to] = ret[names_to].str.replace(names_prefix, '')

    if '.value' in names_to:
        ret2 = ret.pivot(columns='.value', values=values_to)
        rest_columns = list_diff(ret.columns, ['.value', values_to])
        ret2.loc[:, rest_columns] = ret.loc[:, rest_columns]

        ret2_1 = ret2.iloc[:(ret2.shape[0] // 2), ]
        ret2_2 = ret2.iloc[(ret2.shape[0] // 2):, ].reset_index()
        ret = ret2_1.assign(**{
            col: ret2_2[col]
            for col in ret2_1.columns
            if ret2_1[col].isna().all()
        })

    if values_drop_na:
        ret.dropna(subset=[values_to], inplace=True)
    if names_ptypes:
        for key, ptype in names_ptypes.items():
            ret[key] = ret[key].astype(ptype)
    if values_ptypes:
        for key, ptype in values_ptypes.items():
            ret[key] = ret[key].astype(ptype)
    if names_transform:
        for key, tform in names_transform.items():
            ret[key] = ret[key].apply(tform)
    if values_transform:
        for key, tform in values_transform.items():
            ret[key] = ret[key].apply(tform)

    return ret

@register_verb(DataFrame, context=Context.SELECT)
def pivot_wider(
        _data: DataFrame,
        id_cols: Optional[StringOrIter] = None,
        names_from: str = "name",
        names_prefix: str = "",
        names_sep: str = "_",
        names_glue: Optional[str] = None,
        names_sort: bool = False,
        # names_repair: str = "check_unique", # todo
        values_from: StringOrIter = "value",
        values_fill: Any = None,
        values_fn: Optional[Union[Callable, Mapping[str, Callable]]] = None,
) -> DataFrame:
    """"widens" data, increasing the number of columns and decreasing
    the number of rows.

    Args:
        _data: A data frame to pivot.
        id_cols: A set of columns that uniquely identifies each observation.
            Defaults to all columns in data except for the columns specified
            in names_from and values_from.
        names_from, values_from: A pair of arguments describing which column
            (or columns) to get the name of the output column (names_from),
            and which column (or columns) to get the cell values from
            (values_from).
        names_prefix: String added to the start of every variable name.
        names_sep: If names_from or values_from contains multiple variables,
            this will be used to join their values together into a single
            string to use as a column name.
        names_glue: Instead of names_sep and names_prefix, you can supply
            a glue specification that uses the names_from columns
            (and special _value) to create custom column names.
        names_sort: Should the column names be sorted? If FALSE, the default,
            column names are ordered by first appearance.
        names_repair: todo
        values_fill: Optionally, a (scalar) value that specifies what
            each value should be filled in with when missing.
        values_fn: Optionally, a function applied to the value in each cell
            in the output. You will typically use this when the combination
            of id_cols and value column does not uniquely identify
            an observation.
            This can be a dict you want to apply different aggregations to
            different value columns.

    Returns:
        The pivoted dataframe.
    """
    if id_cols is None:
        all_cols = _data.columns.tolist()
        selected_cols = select_columns(all_cols, names_from, values_from)
        id_cols = list_diff(all_cols, selected_cols)
    ret = pandas.pivot_table(
        _data,
        index=id_cols,
        columns=names_from,
        fill_value=values_fill,
        values=values_from,
        aggfunc=values_fn or numpy.mean
    )

    def get_new_colname(cols, names):
        if is_scalar(cols):
            cols = [cols]
        if not names_glue:
            return f'{names_prefix}{names_sep.join(cols)}'
        names = ('_value' if name is None else name for name in names)
        render_data = dict(zip(names, cols))
        return names_glue.format(**render_data)

    new_columns = [
        get_new_colname(col, ret.columns.names)
        for col in ret.columns
    ]
    ret.columns = new_columns
    if names_sort:
        ret = ret.loc[:, sorted(new_columns)]

    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.EVAL)
def uncount(
        _data: DataFrameType,
        weights: IntOrIter,
        _remove: bool = True,
        _id: Optional[str] = None,
) -> DataFrameType:
    """Duplicating rows according to a weighting variable

    Args:
        _data: A data frame
        weights: A vector of weights. Evaluated in the context of data
        _remove: If TRUE, and weights is the name of a column in data,
            then this column is removed.
        _id: Supply a string to create a new variable which gives a
            unique identifier for each created row (0-based).

    Returns:
        dataframe with rows repeated.
    """
    gnames = (
        _data.grouper.names
        if isinstance(_data, DataFrameGroupBy) else None
    )
    _data = objectize(_data)
    if is_scalar(weights):
        weights = [weights] * _data.shape[0]

    indexes = [
        idx for i, idx in enumerate(_data.index)
        for _ in range(weights[i])
    ]

    all_columns = _data.columns.tolist()
    weight_name = getattr(weights, 'name', None)
    if weight_name in all_columns and weights is _data[weight_name]:
        rest_columns = list_diff(all_columns, [weight_name])
    else:
        rest_columns = all_columns

    ret = _data.loc[indexes, rest_columns] if _remove else _data.loc[indexes, :]
    if _id:
        ret = ret.groupby(rest_columns).apply(
            lambda df: df.assign(**{_id: range(df.shape[0])})
        ).reset_index(drop=True, level=0)
    if gnames:
        return ret.groupby(gnames, dropna=False)
    return ret
