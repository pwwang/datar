"""Select helpers"""
import re
import builtins
from typing import Callable, Iterable, List, Union

from pandas import DataFrame
from pipda import register_func
from pipda.utils import functype

from ..core.contexts import Context
from ..core.utils import get_option, vars_select, Array
from ..core.types import StringOrIter
from ..base import setdiff, intersect
from .group_data import group_vars


@register_func(DataFrame, context=Context.EVAL)
def where(_data: DataFrame, fn: Callable) -> List[str]:
    """Selects the variables for which a function returns True.

    Args:
        _data: The data piped in
        fn: A function that returns True or False.
            Currently it has to be `register_func/register_cfunction
            registered function purrr-like formula not supported yet.

    Returns:
        The matched columns
    """
    columns = everything(_data)
    retcols = []
    pipda_type = functype(fn)
    for col in columns:
        if pipda_type == "plain":
            conditions = fn(_data[col])
        else:
            conditions = fn(_data[col], __envdata=_data)

        if isinstance(conditions, bool):
            if conditions:
                retcols.append(col)
            else:
                # pytest-cov not detecting this line
                continue  # pragma: no cover
        elif all(conditions):
            retcols.append(col)

    return retcols


@register_func(DataFrame)
def everything(_data: DataFrame) -> List[str]:
    """Matches all columns.

    Args:
        _data: The data piped in

    Returns:
        All column names of _data
    """
    return setdiff(_data.columns, group_vars(_data))


@register_func(context=Context.SELECT)
def last_col(
    _data: DataFrame,
    offset: int = 0,
    vars: Iterable[str] = None,  # pylint: disable=redefined-builtin
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


@register_func(context=Context.SELECT)
def starts_with(
    _data: DataFrame,
    match: StringOrIter,
    ignore_case: bool = True,
    vars: Iterable[str] = None,  # pylint: disable=redefined-builtin
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


@register_func(context=Context.SELECT)
def ends_with(
    _data: DataFrame,
    match: str,
    ignore_case: bool = True,
    vars: Iterable[str] = None,  # pylint: disable=redefined-builtin
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


@register_func(context=Context.SELECT)
def contains(
    _data: DataFrame,
    match: str,
    ignore_case: bool = True,
    vars: Iterable[str] = None,  # pylint: disable=redefined-builtin
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


@register_func(context=Context.SELECT)
def matches(
    _data: DataFrame,
    match: str,
    ignore_case: bool = True,
    vars: Iterable[str] = None,  # pylint: disable=redefined-builtin
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


@register_func(context=Context.EVAL)
def all_of(
    _data: DataFrame,
    x: Iterable[Union[int, str]],
    base0_: bool = None,
) -> List[str]:
    """For strict selection.

    If any of the variables in the character vector is missing,
    an error is thrown.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns
        base0_: Whether `x` is 0-based or not.
            if not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The matched column names

    Raises:
        ColumnNotExistingError: When any of the elements in `x` does not exist
            in `_data` columns
    """
    all_columns = _data.columns
    x = all_columns[vars_select(all_columns, x, base0=base0_)]
    # where do errors raise?

    # nonexists = setdiff(x, all_columns)
    # if nonexists:
    #     raise ColumnNotExistingError(
    #         "Can't subset columns that don't exist. "
    #         f"Columns {nonexists} not exist."
    #     )

    return list(x)


@register_func(context=Context.SELECT)
def any_of(
    _data: DataFrame,
    x: Iterable[Union[int, str]],
    # pylint: disable=redefined-builtin
    vars: Iterable[str] = None,
    base0_: bool = None,
) -> List[str]:
    """Select but doesn't check for missing variables.

    It is especially useful with negative selections,
    when you would like to make sure a variable is removed.

    Args:
        _data: The data piped in
        x: A set of variables to match the columns
        base0_: Whether `x` is 0-based or not.
            if not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The matched column names
    """
    vars = vars or _data.columns
    x = vars_select(vars, x, raise_nonexists=False, base0=base0_)
    # exists = []
    # for idx in x:
    #     try:
    #         exists.append(vars[idx])
    #     except IndexError:
    #         ...
    # do we need intersect?
    return intersect(vars, [vars[idx] for idx in x])


@register_func(None)
def num_range(
    prefix: str,
    range: Iterable[int],  # pylint: disable=redefined-builtin
    width: int = None,
    base0_: bool = None,
) -> List[str]:
    """Matches a numerical range like x01, x02, x03.

    Args:
        _data: The data piped in
        prefix: A prefix that starts the numeric range.
        range_: A sequence of integers, like `range(3)` (produces `0,1,2`).
        width: Optionally, the "width" of the numeric range.
            For example, a range of 2 gives "01", a range of three "001", etc.
        base0_: Whether it is 0-based

    Returns:
        A list of ranges with prefix.
    """
    base0_ = get_option("index.base.0", base0_)
    zfill = lambda elem: (
        elem + int(not base0_)
        if not width
        else str(elem + int(not base0_)).zfill(width)
    )
    return Array([f"{prefix}{zfill(elem)}" for elem in builtins.range(range)])


def _filter_columns(
    all_columns: Iterable[str],
    match: Union[Iterable[str], str],
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
    if not isinstance(match, (tuple, list, set)):
        match = [match] # type: ignore

    ret = []
    for mat in match:
        for column in all_columns:
            if column in ret:
                continue
            if func(
                mat.lower() if ignore_case else mat,
                column.lower() if ignore_case else column,
            ):
                ret.append(column)

    return ret
