"""Verbs from R-tidyr"""
import re
import itertools
from functools import singledispatch
from typing import Any, Callable, Iterable, Mapping, Optional, Type, Union

import numpy
import pandas
from pandas import DataFrame
from pandas.core.groupby.generic import SeriesGroupBy
from pandas.core.series import Series
from pipda import register_verb

from ..core.utils import (
    copy_attrs, vars_select, logger
)
from ..core.types import (
    DataFrameType, IntOrIter, SeriesLikeType, StringOrIter,
    is_scalar
)
from ..core.middlewares import Nesting
from ..core.contexts import Context
from ..core.names import repair_names
from ..core.grouped import DataFrameGroupBy
from ..base import NA, levels, setdiff
from ..dplyr.distinct import distinct
from ..dplyr.group_by import group_by_drop_default
from ..dplyr.group_data import group_vars

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
        names_sep: and
        names_pattern: If names_to contains multiple values,
            these arguments control how the column name is broken up.
            names_sep takes the same specification as separate(), and
            can either be a numeric vector (specifying positions to break on),
            or a single string (specifying a regular expression to split on).
        names_pattern: takes the same specification as extract(),
            a regular expression containing matching groups (()).
        names_ptypes: and
        values_ptypes: A list of column name-prototype pairs.
            A prototype (or ptype for short) is a zero-length vector
            (like integer() or numeric()) that defines the type, class, and
            attributes of a vector. Use these arguments if you want to confirm
            that the created columns are the types that you expect.
            Note that if you want to change (instead of confirm) the types
            of specific columns, you should use names_transform or
            values_transform instead.
        names_transform: and
        values_transform: A list of column name-function pairs.
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
    all_columns = _data.columns
    columns = all_columns[vars_select(all_columns, cols)]
    id_columns = setdiff(all_columns, columns)
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
        rest_columns = setdiff(ret.columns, ['.value', values_to])
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
        names_from: and
        values_from: A pair of arguments describing which column
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
        all_cols = _data.columns
        selected_cols = all_cols[vars_select(all_cols, names_from, values_from)]
        id_cols = setdiff(all_cols, selected_cols)
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
    if is_scalar(weights):
        weights = [weights] * _data.shape[0]

    indexes = [
        idx for i, idx in enumerate(_data.index)
        for _ in range(weights[i])
    ]

    all_columns = _data.columns.tolist()
    weight_name = getattr(weights, 'name', None)
    if weight_name in all_columns and weights is _data[weight_name]:
        rest_columns = setdiff(all_columns, [weight_name])
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

@singledispatch
def _replace_na(data: Iterable[Any], replace: Any) -> Iterable[Any]:
    """Replace NA for any iterables"""
    return type(data)(replace if pandas.isnull(elem) else elem for elem in data)

@_replace_na.register(numpy.ndarray)
@_replace_na.register(Series)
def _(data: SeriesLikeType, replace: Any) -> SeriesLikeType:
    """Replace NA for numpy.ndarray or Series"""
    ret = data.copy()
    ret[pandas.isnull(ret)] = replace
    return ret

@_replace_na.register(DataFrame)
def _(data: DataFrame, replace: Any) -> DataFrame:
    """Replace NA for numpy.ndarray or DataFrame"""
    return data.fillna(replace)

@_replace_na.register(DataFrameGroupBy)
@_replace_na.register(SeriesGroupBy)
def _(
        data: Union[DataFrameGroupBy, SeriesGroupBy],
        replace: Any
) -> Union[DataFrameGroupBy, SeriesGroupBy]:
    """Replace NA for grouped data, keep the group structure"""
    grouper = data.grouper
    ret = _replace_na(data, replace)
    return ret.groupby(grouper, dropna=False)

@register_verb(
    (DataFrame, DataFrameGroupBy, Series, numpy.ndarray, list, tuple, set),
    context=Context.EVAL
)
def replace_na(
        _data: Iterable[Any],
        series_or_replace: Any,
        replace: Any = None
) -> Any:
    """Replace NA with a value

    This function can be also used not as a verb. As a function called as
    an argument in a verb, _data is passed implicitly. Then one could
    pass series_or_replace as the data to replace.

    Args:
        _data: The data piped in
        series_or_replace: When called as argument of a verb, this is the
            data to replace. Otherwise this is the replacement.
        replace: The value to replace with

    Returns:
        Corresponding data with NAs replaced
    """
    if replace is not None:
        return _replace_na(series_or_replace, replace)
    return _replace_na(_data, series_or_replace)

@register_verb(
    DataFrame,
    context=Context.SELECT
)
def fill(
        _data: DataFrame,
        *columns: str,
        _direction: str = "down"
) -> DataFrame:
    """Fills missing values in selected columns using the next or
    previous entry.

    See https://tidyr.tidyverse.org/reference/fill.html

    Args:
        _data: A dataframe
        *columns: Columns to fill
        _direction: Direction in which to fill missing values.
            Currently either "down" (the default), "up",
            "downup" (i.e. first down and then up) or
            "updown" (first up and then down).

    Returns:
        The dataframe with NAs being replaced.
    """
    data = _data.copy()
    if not columns:
        data = data.fillna(
            method='ffill' if _direction.startswith('down') else 'bfill',
        )
        if _direction in ('updown', 'downup'):
            data = data.fillna(
                method='ffill' if _direction.endswith('down') else 'bfill',
            )
    else:
        columns = data.columns[vars_select(data.columns, *columns)]
        subset = fill(data[columns], _direction=_direction)
        data[columns] = subset
    return data

@fill.register(DataFrameGroupBy, context=Context.SELECT)
def _(
        _data: DataFrameGroupBy,
        *columns: str,
        _direction: str = "down"
) -> DataFrameGroupBy:
    # DataFrameGroupBy
    out = _data.group_apply(
        lambda df: fill(df, *columns, _direction=_direction)
    )
    out = _data.__class__(
        out,
        _group_vars=group_vars(_data),
        _drop=group_by_drop_default(_data)
    )
    copy_attrs(out, _data)
    return out

def expand_grid(
        _data: Iterable[Any] = None,
        _name_repair: str = "check_unique",
        **kwargs: Iterable[Any]
) -> DataFrame:
    """Expand elements into a new dataframe

    See https://tidyr.tidyverse.org/reference/expand_grid.html

    Args:
        _data, **kwargs: Name-value pairs. The name will become the column
            name in the output.
            For _data, will try to fetch name via `_data.__dfname__`. If failed
            `_data` will be used.

    Returns:
        The expanded dataframe
    """
    product_args = []
    names = []
    if isinstance(_data, DataFrame):
        dataname = getattr(_data, '__dfname__', '_data')
        product_args = [(row[1] for row in _data.iterrows())]
        names = [f'{dataname}_{col}' for col in _data.columns]
    elif _data is not None:
        raise ValueError('Positional argument must be a DataFrame or None.')
    for key, val in kwargs.items():
        if isinstance(val, DataFrame):
            product_args.append((row[1] for row in val.iterrows()))
            names.extend(f'{key}_{col}' for col in val.columns)
        else:
            product_args.append(((value, ) for value in val))
            names.append(key)

    return DataFrame(
        (itertools.chain.from_iterable(row)
         for row in itertools.product(*product_args)),
        columns=repair_names(names, _name_repair)
    )

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def extract(
        _data: DataFrameType,
        col: str,
        into: StringOrIter,
        regex: str = r'(\w+)',
        remove: bool = True,
        convert: Union[bool, str, Type, Mapping[str, Union[str, Type]]] = False
) -> DataFrameType:
    """Given a regular expression with capturing groups, extract() turns each
    group into a new column. If the groups don't match, or the input is NA,
    the output will be NA.

    See https://tidyr.tidyverse.org/reference/extract.html

    Args:
        _data: The dataframe
        col: Column name or position.
        into: Names of new variables to create as character vector.
            Use None to omit the variable in the output.
        regex: a regular expression used to extract the desired values.
            There should be one group (defined by ()) for each element of into.
        remove: If TRUE, remove input column from output data frame.
        convert: The universal type for the extracted columns or a dict for
            individual ones

    Returns:
        Dataframe with extracted columns.
    """
    if isinstance(_data, DataFrame):
        if is_scalar(into):
            into = [into]
        colindex = [
            i for i, outcol in enumerate(into)
            if outcol not in (None, NA)
        ]
        extracted = _data[col].str.extract(regex).iloc[:, colindex]
        extracted.columns = [col for col in into if col not in (None, NA)]

        if isinstance(convert, (str, Type)):
            extracted.astype(convert)
        elif isinstance(convert, dict):
            for key, conv in convert.items():
                extracted[key] = extracted[key].astype(conv)
        if remove:
            _data = _data[_data.columns.difference([col])]

        return pandas.concat([_data, extracted], axis=1)

    grouper = _data.grouper
    return _data.apply(
        lambda df: extract(df, col, into, regex, remove, convert)
    ).groupby(grouper, dropna=False)

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def separate( # pylint: disable=too-many-branches
        _data: DataFrameType,
        col: str,
        into: StringOrIter,
        sep: Union[int, str] = r'[^0-9A-Za-z]+',
        remove: bool = True,
        convert: Union[bool, str, Type, Mapping[str, Union[str, Type]]] = False,
        extra: str = "warn",
        fill: str = "warn" # pylint: disable=redefined-outer-name
) -> DataFrameType: # pylint: disable=too-many-nested-blocks
    """Given either a regular expression or a vector of character positions,
    turns a single character column into multiple columns.

    Args:
        _data: The dataframe
        col: Column name or position.
        into: Names of new variables to create as character vector.
            Use None to omit the variable in the output.
        sep: Separator between columns.
            TODO: support index split (sep is an integer)
        remove: If TRUE, remove input column from output data frame.
        convert: The universal type for the extracted columns or a dict for
            individual ones
        extra: If sep is a character vector, this controls what happens when
            there are too many pieces. There are three valid options:
            - "warn" (the default): emit a warning and drop extra values.
            - "drop": drop any extra values without a warning.
            - "merge": only splits at most length(into) times
        fill: If sep is a character vector, this controls what happens when
            there are not enough pieces. There are three valid options:
            - "warn" (the default): emit a warning and fill from the right
            - "right": fill with missing values on the right
            - "left": fill with missing values on the left

    Returns:
        Dataframe with separated columns.
    """
    if isinstance(_data, DataFrame):
        if is_scalar(into):
            into = [into]
        colindex = [
            i for i, outcol in enumerate(into)
            if outcol not in (None, NA)
        ]
        non_na_elems = lambda row: [row[i] for i in colindex]
        # series.str.split can do extra and fill
        # extracted = _data[col].str.split(sep, expand=True).iloc[:, colindex]
        nout = len(into)
        outdata = []
        extra_warns = []
        missing_warns = []
        for i, elem in enumerate(_data[col]):
            if elem in (NA, None):
                row = [NA] * nout
                continue

            row = re.split(sep, str(elem), nout - 1)
            if len(row) < nout:
                if fill == 'warn':
                    missing_warns.append(i)
                if fill in ('warn', 'right'):
                    row += [NA] * (nout - len(row))
                else:
                    row = [NA] * (nout - len(row)) + row
            else:
                more_splits = re.split(sep, row[-1], 1)
                if len(more_splits) > 1:
                    if extra == 'warn':
                        extra_warns.append(i)
                    if extra in ('warn', 'drop'):
                        row[-1] = more_splits[0]

            outdata.append(non_na_elems(row))

        if extra_warns:
            logger.warning(
                'Expected %s pieces. '
                'Additional pieces discarded in %s rows %s.',
                nout,
                len(extra_warns),
                extra_warns
            )
        if missing_warns:
            logger.warning(
                'Expected %s pieces. '
                'Missing pieces filled with `NA` in %s rows %s.',
                nout,
                len(missing_warns),
                missing_warns
            )
        separated = DataFrame(outdata, columns=non_na_elems(into))

        if isinstance(convert, (str, Type)):
            separated.astype(convert)
        elif isinstance(convert, dict):
            for key, conv in convert.items():
                separated[key] = separated[key].astype(conv)
        if remove:
            _data = _data[_data.columns.difference([col])]

        return pandas.concat([_data, separated], axis=1)

    grouper = _data.grouper
    return _data.apply(
        lambda df: separate(df, col, into, sep, remove, convert, extra, fill)
    ).groupby(grouper, dropna=False)


@register_verb(DataFrame, context=Context.SELECT)
def separate_rows(
        _data: DataFrame,
        *columns: str,
        sep: str = r'[^0-9A-Za-z]+',
        convert: Union[bool, str, Type, Mapping[str, Union[str, Type]]] = False,
) -> DataFrame:
    """Separates the values and places each one in its own row.

    Args:
        _data: The dataframe
        *columns: The columns to separate on
        sep: Separator between columns.
        convert: The universal type for the extracted columns or a dict for
            individual ones

    Returns:
        Dataframe with rows separated and repeated.
    """
    all_columns = _data.columns
    selected = all_columns[vars_select(all_columns, *columns)]

    weights = []
    repeated = []
    for row in _data[selected].iterrows():
        row = row[1]
        weights.append(None)
        rdata = []
        for col in selected:
            splits = re.split(sep, row[col])
            if weights[-1] and weights[-1] != len(splits):
                raise ValueError(
                    f'Error: Incompatible lengths: {weights[-1]}, '
                    f'{len(splits)}.'
                )
            weights[-1] = len(splits)
            rdata.append(splits)
        repeated.extend(zip(*rdata))

    ret = uncount(_data, weights)
    ret[selected] = repeated

    if isinstance(convert, (str, Type)):
        ret.astype(convert)
    elif isinstance(convert, dict):
        for key, conv in convert.items():
            ret[key] = ret[key].astype(conv)
    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def unite(
        _data: DataFrameType,
        col: str,
        *columns: str,
        sep: str = '_',
        remove: bool = True,
        na_rm: bool = False
) -> DataFrameType:
    """Unite multiple columns into one by pasting strings together

    Args:
        data: A data frame.
        col: The name of the new column, as a string or symbol.
        *columns: Columns to unite
        sep: Separator to use between values.
        remove: If True, remove input columns from output data frame.
        na_rm: If True, missing values will be remove prior to uniting
            each value.

    Returns:
        The dataframe with selected columns united
    """
    all_columns = _data.columns
    columns = all_columns[vars_select(all_columns, *columns)]

    out = _data.copy()

    def unite_cols(row):
        if na_rm:
            row = [elem for elem in row if elem is not NA]
        return sep.join(str(elem) for elem in row)

    out[col] = out[columns].agg(unite_cols, axis=1)
    if remove:
        out.drop(columns=columns, inplace=True)

    if isinstance(_data, DataFrameGroupBy):
        out = _data.__class__(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )
    copy_attrs(out, _data)
    return out

@register_verb(DataFrame, context=Context.SELECT)
def drop_na(
        _data: DataFrame,
        *columns: str
) -> DataFrame:
    """Drop rows containing missing values

    See https://tidyr.tidyverse.org/reference/drop_na.html

    Args:
        data: A data frame.
        *columns: Columns to inspect for missing values.

    Returns:
        Dataframe with rows with NAs dropped
    """
    all_columns = _data.columns
    columns = vars_select(all_columns, *columns)
    columns = all_columns[columns]
    out = _data.dropna(subset=columns)

    if isinstance(_data, DataFrameGroupBy):
        out = _data.__class__(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )
    copy_attrs(out, _data)
    return out

@register_verb(DataFrame, context=Context.EVAL)
def expand(
        _data: DataFrame, # pylint: disable=no-value-for-parameter
        *columns: Union[str, Nesting],
        # _name_repair: Union[str, Callable] = None # todo
        **kwargs: Iterable[Any]
) -> DataFrame:
    """See https://tidyr.tidyverse.org/reference/expand.html"""
    iterables = []
    names = []
    for i, column in enumerate(columns):
        if isinstance(column, Nesting):
            iterables.append(zip(*column.columns))
            names.extend(column.names)
        else:
            cats = levels(column)
            iterables.append(zip(
                column if cats is None else cats
            ))

            try:
                name = column.name
            except AttributeError:
                name = f'_tmp{hex(id(column))[2:6]}_{i}'
                logger.warning(
                    'Temporary name used. Use keyword argument to '
                    'specify the key as column name.'
                )
            names.append(name)

    for key, val in kwargs.items():
        if isinstance(val, Nesting):
            iterables.append(zip(*val.columns))
            names.extend(f'{key}_{name}' for name in val.names)
        else:
            cats = levels(val)
            iterables.append(zip(
                val if cats is None else cats
            ))
            names.append(key)

    return DataFrame((
        itertools.chain.from_iterable(row)
        for row in itertools.product(*iterables)
    ), columns=names) >> distinct() # pylint: disable=no-value-for-parameter
