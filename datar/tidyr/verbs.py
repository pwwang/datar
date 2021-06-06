"""Verbs from R-tidyr"""
import re
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
from ..core.contexts import Context
from ..core.grouped import DataFrameGroupBy
from ..base import NA, setdiff
from ..dplyr.group_by import group_by_drop_default
from ..dplyr.group_data import group_vars


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
        data_or_replace: Optional[Any] = None,
        replace: Any = None
) -> Any:
    """Replace NA with a value

    This function can be also used not as a verb. As a function called as
    an argument in a verb, _data is passed implicitly. Then one could
    pass data_or_replace as the data to replace.

    Args:
        _data: The data piped in
        data_or_replace: When called as argument of a verb, this is the
            data to replace. Otherwise this is the replacement.
        replace: The value to replace with

    Returns:
        Corresponding data with NAs replaced
    """
    if data_or_replace is None and replace is None:
        return _data.copy()

    if replace is None:
        # no replace, then data_or_replace should be replace
        replace = data_or_replace
    else:
        # replace specified, determine data
        # If data_or_replace is specified, it's data
        _data = _data if data_or_replace is None else data_or_replace

    return _replace_na(_data, data_or_replace)


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
