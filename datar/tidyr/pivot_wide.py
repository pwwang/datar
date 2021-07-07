"""Pivot data from long to wide"""

from typing import List, Any, Union, Callable, Mapping

import pandas
from pandas import DataFrame, Index
from pipda import register_verb

from ..core.contexts import Context
from ..core.types import StringOrIter, is_scalar
from ..core.utils import vars_select, reconstruct_tibble
from ..core.exceptions import ColumnNotExistingError

from ..base import NA, identity
from ..base.na import NA_integer_

ROWID_COLUMN = "_PIVOT_ROWID_"

# pylint: disable=too-many-branches


@register_verb(DataFrame, context=Context.SELECT)
def pivot_wider(
    _data: DataFrame,
    id_cols: StringOrIter = None,
    names_from: StringOrIter = "name",
    names_prefix: str = "",
    names_sep: str = "_",
    names_glue: str = None,
    names_sort: bool = False,
    # names_repair: str = "check_unique", # todo
    values_from: StringOrIter = "value",
    values_fill: Any = None,
    values_fn: Union[Callable, Mapping[str, Callable]] = identity,
    base0_: bool = None,
) -> DataFrame:
    """ "widens" data, increasing the number of columns and decreasing
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
            of `id_cols` and value column does not uniquely identify
            an observation.
            This can be a dict you want to apply different aggregations to
            different value columns.
            If not specified, will be `numpy.mean`
        base0_: Whether `id_cols`, `names_from` and `values_from`
            are 0-based if given by indexes.
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The pivoted dataframe.
    """
    if is_scalar(names_from):
        names_from = [names_from] # type: ignore
    if is_scalar(values_from):
        values_from = [values_from] # type: ignore
    if id_cols is not None and is_scalar(id_cols):
        id_cols = [id_cols] # type: ignore

    if id_cols is None:
        all_cols = _data.columns
        names_from = all_cols[vars_select(all_cols, names_from, base0=base0_)]
        # values_from could be a df-column
        new_values_from = []
        for value_from in values_from:
            if isinstance(value_from, str) and value_from not in all_cols:
                df_cols = [
                    col for col in all_cols if col.startswith(f"{value_from}$")
                ]
                if not df_cols:
                    raise ColumnNotExistingError(value_from)
                new_values_from.extend(df_cols)
            else:
                new_values_from.append(value_from)
        values_from = all_cols[
            vars_select(all_cols, *new_values_from, base0=base0_)
        ]
        id_cols = all_cols.difference(names_from).difference(values_from)

    # build multiindex pivot table
    id_cols = list(id_cols)
    names_from = list(names_from)
    values_from = list(values_from)

    # DF:
    #    id  x  y  a  b
    # 0  10  X  1  1  1
    # 1  20  Y  2  2  2
    #
    # to:
    #    id    a         b
    # x        X    Y    X    Y
    # y        1    2    1    2
    # 0  10  1.0  NaN  1.0  NaN
    # 1  20  NaN  2.0  NaN  2.0
    #
    # with:
    # id_cols = ['id']
    # names_from = ['x', 'y']
    # values_from = ['a', 'b']
    #
    # expected:
    #     id a_X_1 a_Y_2 b_X_1 b_Y_2
    # 0   10 1     NaN   1     NaN
    # 1   20 NaN   2     NaN   2
    if len(id_cols) == 0 and len(values_from) > 1:
        # need to add it to turn names_to to columns
        ret = _data.assign(**{ROWID_COLUMN: 0})
        id_cols = [ROWID_COLUMN]
    else:
        ret = _data

    # hold NAs in values_from columns, so that they won't be filled
    # by values_fill
    for col in values_from:
        ret[col].fillna(NA_integer_, inplace=True)

    ret = pandas.pivot_table(
        ret,
        index=id_cols,
        columns=names_from,
        fill_value=values_fill,
        values=values_from[0] if len(values_from) == 1 else values_from,
        aggfunc=values_fn,
    )

    ret.columns = _flatten_column_names(
        ret.columns, names_prefix, names_sep, names_glue
    )

    if len(id_cols) > 0:
        ret.reset_index(inplace=True)

    if ROWID_COLUMN in ret:
        ret.drop(columns=[ROWID_COLUMN], inplace=True)

    ret.reset_index(drop=True, inplace=True)
    # Get the original NAs back
    for col in ret.columns.difference(id_cols):
        ret[col].replace({NA_integer_: NA}, inplace=True)

    if names_sort:
        ret = ret.loc[:, sorted(ret.columns)]

    return reconstruct_tibble(_data, ret)


def _flatten_column_names(
    names: Index, names_prefix: str, names_sep: str, names_glue: str
) -> List[str]:
    """Flatten the hierachical column names:

    For example,
        >>> MultiIndex([('id',  '', ''),
        >>>    ( 'a', 'X',  1),
        >>>    ( 'a', 'Y',  2),
        >>>    ( 'b', 'X',  1),
        >>>    ( 'b', 'Y',  2)],
        >>>     names=[None, 'x', 'y'])
    To
        >>> ['X1_a', 'Y2_a', 'X1_b', 'Y2_b']
    with `names_glue={x}{y}_{_value}`
    """
    lvlnames = ["_value" if level is None else level for level in names.names]
    out = []
    for cols in names:
        if is_scalar(cols):
            cols = [cols]
            # out.append(f'{names_prefix}{cols}')
            # continue
        # if len(cols) == 1:
        #     out.append(f'{names_prefix}{cols[0]}')
        #     continue

        cols = dict(zip(lvlnames, (str(col) for col in cols)))
        # in case of ('id', '', '')
        # if all(name == '' for key, name in cols.items() if key != '_value'):
        #     out.append(f'{names_prefix}{cols["_value"]}')
        # in case of values_from is a dataframe column
        # ('d$a', 'X', '1')
        if "$" in cols.get("_value", ""):
            prefix = names_prefix + names_sep.join(
                col for name, col in cols.items() if name != "_value"
            )
            out.append(f'{prefix}${cols["_value"].split("$", 1)[1]}')
        elif not names_glue:
            out.append(f"{names_prefix}{names_sep.join(cols.values())}")
        else:
            if "_value" in cols:
                cols[".value"] = cols["_value"]
            out.append(names_glue.format(**cols))

    return out
