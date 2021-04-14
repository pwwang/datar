"""Select helpers"""
from typing import Callable, Iterable, List, Optional
from pandas import DataFrame
from pipda import register_func
from pipda.utils import functype

from ..core.contexts import Context
from ..base.funcs import setdiff
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
        if pipda_type == 'plain':
            conditions = fn(_data[col])
        elif pipda_type == 'plain-func':
            conditions = fn(_data[col], _env=_data)
        else:
            conditions = fn(_data, _data[col], _env=_data)

        if isinstance(conditions, bool):
            if conditions:
                retcols.append(col)
            else:
                continue
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
        vars: Optional[Iterable[str]] = None # pylint: disable=redefined-builtin
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
    return vars[-(offset+1)]