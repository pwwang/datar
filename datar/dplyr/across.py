"""Apply a function (or functions) across multiple columns

See source https://github.com/tidyverse/dplyr/blob/master/R/across.R
"""
from typing import Any, Callable, Iterable, Mapping, Optional, Union

import numpy
from pandas import DataFrame, Series
from pipda import register_func
from pipda.context import ContextBase

from ..core.utils import vars_select
from ..core.contexts import Context
from ..core.middlewares import Across, IfAll, IfAny

@register_func(
    context=None,
    extra_contexts={'_cols': Context.SELECT},
    verb_arg_only=True
)
def across(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[
            Callable,
            Iterable[Callable],
            Mapping[str, Callable]
        ]] = None,
        _names: Optional[str] = None,
        _context: Optional[ContextBase] = None,
        **kwargs: Any
) -> DataFrame:
    """Apply the same transformation to multiple columns

    The original API:
    https://dplyr.tidyverse.org/reference/across.html

    Args:
        _data: The dataframe
        _cols: The columns
        _fns: Functions to apply to each of the selected columns.
        _names: A glue specification that describes how to name
            the output columns. This can use `{_col}` to stand for the
            selected column name, and `{_fn}` to stand for the name of
            the function being applied.
            The default (None) is equivalent to `{_col}` for the
            single function case and `{_col}_{_fn}` for the case where
            a list is used for _fns. In such a case, `{_fn}` is 0-based.
            To use 1-based index, use `{_fn1}`
        **kwargs: Arguments for the functions

    Returns:
        A dataframe with one column for each column in _cols and
        each function in _fns.
    """
    return Across(_data, _cols, _fns, _names, kwargs).evaluate(_context)

@register_func(context=Context.SELECT, verb_arg_only=True)
def c_across(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None
) -> Series:
    """Apply the same transformation to multiple columns rowwisely

    Args:
        _data: The dataframe
        _cols: The columns

    Returns:
        A series
    """
    if not _cols:
        _cols = _data.columns
    _cols = vars_select(_data.columns.tolist(), _cols)

    series = [_data.iloc[:, col] for col in _cols]
    return numpy.concatenate(series)

@register_func(
    context=None,
    extra_contexts={'_cols': Context.SELECT},
    verb_arg_only=True
)
def if_any(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        _context: Optional[ContextBase] = None,
        **kwargs: Any
) -> Iterable[bool]:
    """apply the same predicate function to a selection of columns and combine
    the results True if any element is True.

    See across().
    """
    return IfAny(_data, _cols, _fns, _names, kwargs).evaluate(_context)


@register_func(
    context=None,
    extra_contexts={'_cols': Context.SELECT},
    verb_arg_only=True
)
def if_all(
        _data: DataFrame,
        _cols: Optional[Iterable[str]] = None,
        _fns: Optional[Union[Mapping[str, Callable]]] = None,
        _names: Optional[str] = None,
        _context: Optional[ContextBase] = None,
        **kwargs: Any
) -> Iterable[bool]:
    """apply the same predicate function to a selection of columns and combine
    the results True if all elements are True.

    See across().
    """
    return IfAll(_data, _cols, _fns, _names, kwargs).evaluate(_context)
