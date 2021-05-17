"""Apply a function (or functions) across multiple columns

See source https://github.com/tidyverse/dplyr/blob/master/R/across.R
"""
import builtins
from abc import ABC
from typing import Any, Callable, Iterable, Mapping, Optional, Union

import numpy
from pandas import DataFrame, Series
from pipda import register_func
from pipda.utils import functype
from pipda.context import ContextBase
from pipda.symbolic import DirectRefAttr

from ..core.utils import df_assign_item, to_df, vars_select
from ..core.middlewares import CurColumn
from ..core.contexts import Context
from .tidyselect import everything

class Across:
    """Across object"""
    def __init__(
            self,
            data: DataFrame,
            cols: Optional[Iterable[str]] = None,
            fns: Optional[Union[
                Callable,
                Iterable[Callable],
                Mapping[str, Callable]
            ]] = None,
            names: Optional[str] = None,
            kwargs: Optional[Mapping[str, Any]] = None
    ) -> None:
        cols = everything(data) if cols is None else cols
        if not isinstance(cols, (list, tuple)):
            cols = [cols]
        cols = data.columns[vars_select(data.columns, *cols)]

        fns_list = []
        if callable(fns):
            fns_list.append({'fn': fns})
        elif isinstance(fns, (list, tuple)):
            fns_list.extend(
                {'fn': fn, '_fn': i, '_fn1': i+1}
                for i, fn in enumerate(fns)
            )
        elif isinstance(fns, dict):
            fns_list.extend(
                {'fn': value, '_fn': key}
                for key, value in fns.items()
            )
        elif fns is not None:
            raise ValueError(
                'Argument `_fns` of across must be None, a function, '
                'a formula, or a dict of functions.'
            )

        self.data = data
        self.cols = cols
        self.fns = fns_list
        self.names = names
        self.kwargs = kwargs or {}

    def evaluate(self, context: Optional[ContextBase] = None) -> DataFrame:
        """Evaluate object with context"""
        if not self.fns:
            self.fns = [{'fn': lambda x: x}]

        ret = None
        for column in self.cols:
            for fn_info in self.fns:
                render_data = fn_info.copy()
                render_data['_col'] = column
                fn = render_data.pop('fn')
                name_format = self.names
                if not name_format:
                    name_format = (
                        '{_col}_{_fn}' if '_fn' in render_data
                        else '{_col}'
                    )

                name = name_format.format(**render_data)
                if functype(fn) == 'plain':
                    value = fn(
                        self.data[column],
                        **CurColumn.replace_kwargs(self.kwargs, column)
                    )
                else:
                    # use fn's own context
                    value = fn(
                        DirectRefAttr(self.data, column),
                        **CurColumn.replace_kwargs(self.kwargs, column),
                        _env='piping'
                    )(self.data, context)

                # todo: check if it is proper
                #       group information lost
                if ret is None:
                    ret = to_df(value, name)
                else:
                    df_assign_item(ret, name, value)
        return DataFrame() if ret is None else ret

class IfCross(Across, ABC):
    """Base class for IfAny and IfAll"""
    if_type = None

    def evaluate(self, context: Optional[ContextBase] = None) -> DataFrame:
        """Evaluate the object with context"""
        agg_func = getattr(builtins, self.__class__.if_type)
        return super().evaluate(context).fillna(
            False
        ).astype(
            'boolean'
        ).apply(agg_func, axis=1)

class IfAny(IfCross):
    """For calls from dplyr's if_any"""
    if_type = 'any'

class IfAll(IfCross):
    """For calls from dplyr's if_all"""
    if_type = 'all'

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
        _cols = everything(_data)

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
