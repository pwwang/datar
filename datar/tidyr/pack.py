"""Pack and unpack

https://github.com/tidyverse/tidyr/blob/master/R/pack.R
"""
from typing import Iterable, Set, Optional, Union, Callable

from pandas import DataFrame
from pipda import register_verb

from ..core.utils import vars_select, copy_attrs
from ..core.grouped import DataFrameGroupBy
from ..core.contexts import Context
from ..core.types import StringOrIter, IntOrIter, is_scalar
from ..core.names import repair_names

from ..base import setdiff, intersect
from ..dplyr import bind_cols, group_vars, group_by_drop_default

@register_verb(DataFrame, context=Context.SELECT)
def pack(
        _data: DataFrame,
        _names_sep: Optional[str] = None,
        _base0: Optional[bool] = None,
        **cols: Union[str, int]
) -> DataFrame:
    """Makes df narrow by collapsing a set of columns into a single df-column.

    Args:
        _data: A data frame
        **cols: Columns to pack
        _names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `_names_sep`.
        _base0: Whether `**cols` are 0-based
            if not provided, will use `datar.base.getOption('index.base.0')`
    """
    if not cols:
        return _data.copy()

    from .nest import _strip_names
    all_columns = _data.columns
    colgroups = {}
    usedcols = set()
    for group, columns in cols.items():
        oldcols = all_columns[vars_select(all_columns, columns, base0=_base0)]
        usedcols = usedcols.union(oldcols)
        newcols = (
            oldcols if _names_sep is None else
            _strip_names(oldcols, group, _names_sep)
        )
        colgroups[group] = zip(newcols, oldcols)

    cols = {}
    for group, columns in colgroups.items():
        for newcol, oldcol in columns:
            cols[f'{group}${newcol}'] = _data[oldcol]

    asis = setdiff(_data.columns, usedcols)
    out = bind_cols(_data[asis], DataFrame(cols))
    if isinstance(_data, DataFrameGroupBy):
        out = _data.__class__(
            out,
            _group_vars=intersect(group_vars(_data), out.columns),
            _drop=group_by_drop_default(_data)
        )

    copy_attrs(out, _data)
    return out

@register_verb(DataFrame, context=Context.SELECT)
def unpack(
        data: DataFrame,
        cols: Union[StringOrIter, IntOrIter],
        names_sep: Optional[str] = None,
        names_repair: Union[str, Callable] = "check_unique",
        _base0: Optional[bool] = None
) -> DataFrame:
    """Makes df wider by expanding df-columns back out into individual columns.

    For empty columns, the column is kept asis, instead of removing it.

    Args:
        data: A data frame
        cols: Columns to unpack
        names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `_names_sep`.
        name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        _base0: Whether `cols` are 0-based
            if not provided, will use `datar.base.getOption('index.base.0')`

    Returns:
        Data frame with given columns unpacked.
    """
    if is_scalar(cols):
        cols = [cols]

    all_columns = data.columns
    cols = _check_present(
        data, cols, all_columns,
        base0=_base0,
    )

    out = data.copy()
    new_cols = []
    for col in data.columns:
        if '$' in col:
            parts = col.split('$', 1)
            if parts[0] not in cols:
                new_cols.append(col)
            else:
                replace = "" if names_sep is None else f"{parts[0]}{names_sep}"
                new_cols.append(f"{replace}{parts[1]}")
        # elif col in cols: # empty list column
        #     # remove it from out
        #     out.drop(columns=col, inplace=True)
        else:
            new_cols.append(col)

    new_cols = repair_names(new_cols, names_repair, _base0)
    out.columns = new_cols

    copy_attrs(out, data)
    return out


def _check_present(
        data: DataFrame,
        cols: Iterable[Union[int, str]],
        all_columns: Iterable[str],
        base0: Optional[bool] = None
) -> Set[str]:
    """Check if cols are packed columns"""
    out = set()
    for col in cols:
        if not isinstance(col, str):
            columns = vars_select(all_columns, col, base0=base0)
            columns = all_columns[columns][0].split('$', 1)[0]
        else:
            columns = [col]

        for column in columns:
            if not (column in data and len(data[column]) == 0) and not any(
                    allcol.startswith(f"{column}$") for allcol in all_columns
            ):
                raise ValueError(f"`{column}` must be a data frame column.")

            if column in out:
                raise ValueError(
                    f"`{column}` has already been selected. "
                    "Number of packed columns also counts when "
                    "selecting using indexes."
                )
            out.add(column)
    return out
