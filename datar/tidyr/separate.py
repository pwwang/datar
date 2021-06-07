"""Separate a character column into multiple columns with a regular
expression or numeric locations

https://github.com/tidyverse/tidyr/blob/HEAD/R/separate.R
"""
import re
from typing import Any, List, Union, Mapping, Optional

import pandas
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.types import DTypeType, StringOrIter, is_scalar
from ..core.utils import logger, vars_select, apply_dtypes, position_at
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise

from ..base import NA, setdiff, intersect
from ..dplyr import group_vars, group_by_drop_default, ungroup, mutate

from .chop import unchop

@register_verb(DataFrame, context=Context.SELECT)
def separate(
        data: DataFrame,
        col: Union[str, int],
        into: StringOrIter,
        sep: Union[int, str] = r'[^0-9A-Za-z]+',
        remove: bool = True,
        convert: Union[bool, DTypeType, Mapping[str, DTypeType]] = False,
        extra: str = "warn",
        fill: str = "warn",
        _base0: Optional[bool] = None
) -> DataFrame:
    """Given either a regular expression or a vector of character positions,
    turns a single character column into multiple columns.

    Args:
        data: The dataframe
        col: Column name or position.
        into: Names of new variables to create as character vector.
            Use `None`/`NA`/`NULL` to omit the variable in the output.
        sep: Separator between columns.
            If str, `sep` is interpreted as a regular expression.
            The default value is a regular expression that matches
            any sequence of non-alphanumeric values.
            If int, `sep` is interpreted as character positions to split at.
        remove: If TRUE, remove input column from output data frame.
        convert: The universal type for the extracted columns or a dict for
            individual ones
            Note that when given `TRUE`, `DataFrame.convert_dtypes()` is called,
            but it will not convert `str` to other types
            (For example, `'1'` to `1`). You have to specify the dtype yourself.
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
        _base0: Whether `col` is 0-based when given by index and Whether `sep`
            is 0-based if given by position
            If not provided, will use `datar.base.getOption('index.base.0')`

    Returns:
        Dataframe with separated columns.
    """
    if is_scalar(into):
        into = [into]

    if not all(isinstance(it, str) or pandas.isnull(it) for it in into):
        raise ValueError("`into` must be a string or a list of strings.")

    all_columns = data.columns
    col = vars_select(all_columns, col, base0=_base0)
    col = all_columns[col[0]]

    colindex = [
        i for i, outcol in enumerate(into)
        if not pandas.isnull(outcol)
    ]
    non_na_elems = lambda row: [row[i] for i in colindex]
    # series.str.split can't do extra and fill
    # extracted = data[col].str.split(sep, expand=True).iloc[:, colindex]
    nout = len(into)
    extra_warns = []
    missing_warns = []

    separated = data[col].apply(
        _separate_col,
        nout=nout,
        sep=sep,
        extra=extra,
        fill=fill,
        base0=_base0,
        extra_warns=extra_warns,
        missing_warns=missing_warns
    )

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

    separated = DataFrame(separated.values.tolist()).iloc[:, colindex]
    separated.columns = non_na_elems(into)
    apply_dtypes(separated, convert)

    out = data.drop(columns=[col]) if remove else data
    out = mutate(out, separated)

    if (
            isinstance(data, DataFrameGroupBy) and
            not isinstance(data, DataFrameRowwise)
    ):
        gvars = intersect(group_vars(data), out.columns)

        if len(gvars) > 0:
            return DataFrameGroupBy(
                out,
                _group_vars=gvars,
                _drop=group_by_drop_default(data)
            )

    return out

@register_verb(DataFrame, context=Context.SELECT)
def separate_rows(
        data: DataFrame,
        *columns: str,
        sep: str = r'[^0-9A-Za-z]+',
        convert: Union[bool, DTypeType, Mapping[str, DTypeType]] = False,
        _base0: Optional[bool] = None
) -> DataFrame:
    """Separates the values and places each one in its own row.

    Args:
        data: The dataframe
        *columns: The columns to separate on
        sep: Separator between columns.
        convert: The universal type for the extracted columns or a dict for
            individual ones
        _base0: Whether `columns` is 0-based when given by index
            If not provided, will use `datar.base.getOption('index.base.0')`

    Returns:
        Dataframe with rows separated and repeated.
    """
    all_columns = data.columns
    selected = all_columns[vars_select(all_columns, *columns, base0=_base0)]
    out = data.copy()
    for sel in selected:
        out[sel] = out[sel].apply(
            _separate_col,
            nout=0,
            sep=sep,
            extra="merge",
            fill="right",
            base0=_base0,
            extra_warns=[],
            missing_warns=[]
        )

    out = unchop(out, selected, keep_empty=True, dtypes=convert, _base0=_base0)
    gvars_exclude = intersect(selected, group_vars(out))
    if len(gvars_exclude) > 0:
        gvars = setdiff(group_vars(out), gvars_exclude)
        if len(gvars) == 0:
            return ungroup(out)

        return out.__class__(
            out,
            _group_vars=gvars,
            _drop=group_by_drop_default(data)
        )

    return out

def _separate_col(
        elem: Any,
        nout: int,
        sep: Union[str, int],
        extra: str,
        fill: str,
        base0: Optional[bool],
        # pylint: disable=dangerous-default-value
        extra_warns: List[str] = [], # mutatable to save warnings
        missing_warns: List[str] = []
) -> List[Optional[str]]:
    """Separate the column"""
    if (is_scalar(elem) and pandas.isnull(elem)) or (
            not is_scalar(elem) and any(pandas.isnull(elem))
    ):
        return [NA] * nout if nout > 0 else NA

    elem = str(elem)
    if isinstance(sep, int):
        try:
            tmp = position_at(sep, len(elem), base0)
        except IndexError:
            tmp = 0 if sep < 0 else len(elem) - 1
        tmp = sep - 1 if sep < 0 else tmp
        row = [elem[:tmp+1], elem[tmp+1:]]
    else:
        row = re.split(sep, elem, 0 if nout == 0 else nout - 1)
    if nout == 0:
        return row
    if len(row) < nout:
        if fill == 'warn' and (
                not missing_warns or missing_warns[-1] != '...truncated'
        ):
            missing_warns.append(elem)
        if fill in ('warn', 'right'):
            row += [NA] * (nout - len(row))
        else:
            row = [NA] * (nout - len(row)) + row
    elif not isinstance(sep, int):
        more_splits = re.split(sep, row[-1], 1)
        if len(more_splits) > 1:
            if extra == 'warn' and (
                    not extra_warns or extra_warns[-1] != '...truncated'
            ):
                extra_warns.append(elem)
            if extra in ('warn', 'drop'):
                row[-1] = more_splits[0]
    return row
