"""Select helpers"""
import re
import builtins
from typing import Callable, List, Sequence, Union

import numpy as np
from pipda import register_func, register_verb, Function, Verb

from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.api.types import is_scalar, is_bool

from ..core.contexts import Context
from ..core.utils import ensure_nparray, vars_select
from ..base import setdiff, intersect
from .group_by import ungroup
from .group_data import group_vars


@register_verb(DataFrame, context=Context.EVAL, dep=True)
def where(_data: DataFrame, fn: Callable) -> List[str]:
    """Selects the variables for which a function returns True.

    Args:
        _data: The data piped in
        fn: A function that returns True or False.
            Currently it has to be `register_func/func_factory
            registered function purrr-like formula not supported yet.

    Returns:
        The matched columns
    """
    columns = _data >> everything()
    _data = ungroup(_data, __ast_fallback="normal")
    mask = []
    for col in columns:
        if isinstance(fn, Function):
            dat = fn(_data[col])
            mask.append(dat)
        elif isinstance(fn, Verb):
            dat = fn(_data, _data[col], __ast_fallback="normal")
            mask.append(dat)
        else:
            mask.append(fn(_data[col]))

    mask = [flag if is_bool(flag) else all(flag) for flag in mask]
    return np.array(columns)[mask].tolist()


@register_verb(DataFrame, dep=True)
def everything(_data: DataFrame) -> List[str]:
    """Matches all columns.

    Args:
        _data: The data piped in

    Returns:
        All column names of _data
    """
    return list(
        setdiff(
            _data.columns,
            group_vars(_data, __ast_fallback="normal"),
            __ast_fallback="normal",
        )
    )


@register_verb(DataFrame, context=Context.SELECT, dep=True)
def last_col(
    _data: DataFrame,
    offset: int = 0,
    vars: Sequence[str] = None,
) -> str:
    """Select last variable, possibly with an offset.

    Args:
        _data: The data piped in
        offset: The offset from the end.
            Note that this is 0-based, the same as `tidyverse`'s `last_col`
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        The variable
    """
    vars = vars or _data.columns
    return vars[-(offset + 1)]


@register_verb(DataFrame, context=Context.SELECT, dep=True)
def starts_with(
    _data: DataFrame,
    match: Union[str, Sequence[str]],
    ignore_case: bool = True,
    vars: Sequence[str] = None,
) -> List[str]:
    """Select columns starting with a prefix.

    Args:
        _data: The data piped in
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return _filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        lambda mat, cname: cname.startswith(mat),
    )


@register_verb(DataFrame, context=Context.SELECT, dep=True)
def ends_with(
    _data: DataFrame,
    match: Union[str, Sequence[str]],
    ignore_case: bool = True,
    vars: Sequence[str] = None,
) -> List[str]:
    """Select columns ending with a suffix.

    Args:
        _data: The data piped in
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return _filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        lambda mat, cname: cname.endswith(mat),
    )


@register_verb(DataFrame, context=Context.SELECT, dep=True)
def contains(
    _data: DataFrame,
    match: str,
    ignore_case: bool = True,
    vars: Sequence[str] = None,
) -> List[str]:
    """Select columns containing substrings.

    Args:
        _data: The data piped in
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return _filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        lambda mat, cname: mat in cname,
    )


@register_verb(DataFrame, context=Context.SELECT, dep=True)
def matches(
    _data: DataFrame,
    match: str,
    ignore_case: bool = True,
    vars: Sequence[str] = None,
) -> List[str]:
    """Select columns matching regular expressions.

    Args:
        _data: The data piped in
        match: Regular expressions. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        vars: A set of variable names. If not supplied, the variables are
            taken from the data columns.

    Returns:
        A list of matched vars
    """
    return _filter_columns(
        vars or _data.columns,
        match,
        ignore_case,
        re.search,
    )


@register_verb(DataFrame, context=Context.EVAL, dep=True)
def all_of(
    _data: DataFrame,
    x: Sequence[Union[int, str]],
) -> List[str]:
    """For strict selection.

    If any of the variables in the character vector is missing,
    an error is thrown.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns

    Returns:
        The matched column names

    Raises:
        ColumnNotExistingError: When any of the elements in `x` does not exist
            in `_data` columns
    """
    all_columns = _data.columns
    x = all_columns[vars_select(all_columns, x)]
    # where do errors raise?

    # nonexists = setdiff(x, all_columns)
    # if nonexists:
    #     raise ColumnNotExistingError(
    #         "Can't subset columns that don't exist. "
    #         f"Columns {nonexists} not exist."
    #     )

    return x.tolist()


@register_verb(DataFrame, context=Context.SELECT, dep=True)
def any_of(
    _data: DataFrame,
    x: Sequence[Union[int, str]],
    vars: Sequence[str] = None,
) -> List[str]:
    """Select but doesn't check for missing variables.

    It is especially useful with negative selections,
    when you would like to make sure a variable is removed.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns

    Returns:
        The matched column names
    """
    vars = vars or _data.columns
    x = vars_select(vars, x, raise_nonexists=False)
    # exists = []
    # for idx in x:
    #     try:
    #         exists.append(vars[idx])
    #     except IndexError:
    #         ...
    # do we need intersect?
    return list(
        intersect(
            vars,
            ensure_nparray(vars)[x],
            __ast_fallback="normal",
        )
    )


@register_func
def num_range(
    prefix: str,
    range: Sequence[int],
    width: int = None,
) -> List[str]:
    """Matches a numerical range like x01, x02, x03.

    Args:
        _data: The data piped in
        prefix: A prefix that starts the numeric range.
        range_: A sequence of integers, like `range(3)` (produces `0,1,2`).
        width: Optionally, the "width" of the numeric range.
            For example, a range of 2 gives "01", a range of three "001", etc.

    Returns:
        A list of ranges with prefix.
    """
    zfill = lambda elem: (elem if not width else str(elem).zfill(width))
    return [f"{prefix}{zfill(elem)}" for elem in builtins.range(range)]


def _filter_columns(
    all_columns: Sequence[str],
    match: Union[Sequence[str], str],
    ignore_case: bool,
    func: Callable[[str, str], bool],
) -> List[str]:
    """Filter the columns with given critera

    Args:
        all_columns: The column pool to filter
        match: Strings. If len>1, the union of the matches is taken.
        ignore_case: If True, the default, ignores case when matching names.
        func: A function to define how to filter.

    Returns:
        A list of matched vars
    """
    if is_scalar(match):
        match = [match]  # type: ignore

    ret = []
    for mat in match:  # order kept this way
        for column in all_columns:
            if column in ret:
                continue
            if func(
                mat.lower() if ignore_case else mat,
                column.lower() if ignore_case else column,
            ):
                ret.append(column)

    return ret
