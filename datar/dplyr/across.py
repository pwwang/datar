"""Apply a function (or functions) across multiple columns

See source https://github.com/tidyverse/dplyr/blob/master/R/across.R
"""
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, List, Mapping, Tuple, Union

import numpy
from pandas import DataFrame, Series
from pipda import register_func, evaluate_expr
from pipda.function import Function
from pipda.utils import functype, CallingEnvs
from pipda.context import ContextBase

from ..core.utils import (
    df_setitem,
    length_of,
    recycle_df,
    to_df,
    vars_select,
    get_option,
)
from ..core.middlewares import CurColumn
from ..core.contexts import Context
from .tidyselect import everything


class Across:
    """Across object"""

    def __init__(
        self,
        data: DataFrame,
        cols: Iterable[str] = None,
        fns: Union[Callable, Iterable[Callable], Mapping[str, Callable]] = None,
        names: str = None,
        base0: bool = None,
        args: Tuple[Any] = None,
        kwargs: Mapping[str, Any] = None,
    ) -> None:
        cols = everything(data) if cols is None else cols
        if not isinstance(cols, (list, tuple)):
            cols = [cols]
        cols = data.columns[vars_select(data.columns, cols, base0=base0)]
        base0 = get_option("index.base.0", base0)

        fns_list = [] # type: List[str, Union[int, Callable]]
        if callable(fns):
            fns_list.append({"fn": fns})
        elif isinstance(fns, (list, tuple)):
            fns_list.extend(
                {"fn": fn, "_fn": i + int(not base0), "_fn1": i + 1, "_fn0": i}
                for i, fn in enumerate(fns)
            )
        elif isinstance(fns, dict):
            fns_list.extend(
                {"fn": value, "_fn": key} for key, value in fns.items()
            )
        elif fns is not None:
            raise ValueError(
                "Argument `_fns` of across must be None, a function, "
                "a formula, or a dict of functions."
            )

        self.data = data
        self.cols = cols
        self.fns = fns_list
        self.names = names
        self.args = args or ()
        self.kwargs = kwargs or {}

    def evaluate(
        self, context: Union[Context, ContextBase] = None
    ) -> DataFrame:
        """Evaluate object with context"""
        if isinstance(context, Context):
            context = context.value

        if not self.fns:
            self.fns = [{"fn": lambda x: x}]
        ret = None
        for column in self.cols:
            for fn_info in self.fns:
                render_data = fn_info.copy()
                render_data["_col"] = column
                fn = render_data.pop("fn")
                name_format = self.names
                if not name_format:
                    name_format = (
                        "{_col}_{_fn}" if "_fn" in render_data else "{_col}"
                    )

                name = name_format.format(**render_data)
                args = CurColumn.replace_args(self.args, column)
                kwargs = CurColumn.replace_kwargs(self.kwargs, column)
                if functype(fn) == "plain":
                    value = fn(
                        self.data[column],
                        *evaluate_expr(args, self.data, context),
                        **evaluate_expr(kwargs, self.data, context),
                    )
                else:
                    # use fn's own context
                    value = fn(
                        self.data[column],
                        *args,
                        **kwargs,
                        __calling_env=CallingEnvs.PIPING,
                    )
                    # fast evaluation tried, if failed:
                    # will this happen? it fails when first argument
                    # cannot be evaluated
                    if isinstance(value, Function):  # pragma: no cover
                        value = value._pipda_eval(self.data, context)

                if ret is None:
                    ret = to_df(value, name)
                elif length_of(ret) == 1:
                    ret, value = recycle_df(ret, value)
                    ret = df_setitem(ret, name, value)
                else:
                    ret = df_setitem(ret, name, value)
        return DataFrame() if ret is None else ret


class IfCross(Across, ABC):
    """Base class for IfAny and IfAll"""

    @staticmethod
    @abstractmethod
    def aggregate(values: Series) -> bool:
        """How to aggregation by rows"""

    def evaluate(self, context: ContextBase = None) -> DataFrame:
        """Evaluate the object with context"""
        # Fill NA first and then do and/or
        # Since NA | True -> False for pandas
        return (
            super()
            .evaluate(context)
            .apply(self.__class__.aggregate, axis=1)
            .astype(bool)
        )


class IfAny(IfCross):
    """For calls from dplyr's if_any"""

    @staticmethod
    def aggregate(values: Series) -> bool:
        """How to aggregation by rows"""
        return values.fillna(False).astype(bool).any()


class IfAll(IfCross):
    """For calls from dplyr's if_all"""

    @staticmethod
    def aggregate(values: Series) -> bool:
        """How to aggregation by rows"""
        return values.fillna(False).astype(bool).all()


@register_func(
    context=Context.PENDING, verb_arg_only=True, summarise_prefers_input=True
)
def across(
    _data: DataFrame,
    *args: Any,
    _names: str = None,
    _fn_context: Union[Context, ContextBase] = Context.EVAL,
    base0_: bool = None,
    **kwargs: Any,
) -> DataFrame:
    """Apply the same transformation to multiple columns

    The original API:
    https://dplyr.tidyverse.org/reference/across.html

    Args:
        _data: The dataframe.
        *args: If given, the first 2 elements should be columns and functions
            apply to each of the selected columns. The rest of them will be
            the arguments for the functions.
        _names: A glue specification that describes how to name
            the output columns. This can use `{_col}` to stand for the
            selected column name, and `{_fn}` to stand for the name of
            the function being applied.
            The default (None) is equivalent to `{_col}` for the
            single function case and `{_col}_{_fn}` for the case where
            a list is used for _fns. In such a case, `{_fn}` is 0-based.
            To use 1-based index, use `{_fn1}`
        base0_: Indicating whether the columns are 0-based if selected
            by indexes. if not provided, will use
            `datar.base.get_option('index.base.0')`.
        _fn_context: Defines the context to evaluate the arguments for functions
            if they are plain functions.
            Note that registered functions will use its own context
        **kwargs: Keyword arguments for the functions

    Returns:
        A dataframe with one column for each column and each function.
    """
    if not args:
        args = (None, None)
    elif len(args) == 1:
        args = (args[0], None)
    _cols, _fns, *args = args
    _cols = evaluate_expr(_cols, _data, Context.SELECT)

    return Across(_data, _cols, _fns, _names, base0_, args, kwargs).evaluate(
        _fn_context
    )


@register_func(context=Context.SELECT, verb_arg_only=True)
def c_across(
    _data: DataFrame,
    _cols: Iterable[str] = None,
    base0_: bool = None,
) -> Series:
    """Apply the same transformation to multiple columns rowwisely

    Args:
        _data: The dataframe
        _cols: The columns
        base0_: Indicating whether the columns are 0-based if selected
            by indexes. if not provided, will use
            `datar.base.get_option('index.base.0')`.

    Returns:
        A series
    """
    if not _cols:
        _cols = everything(_data)

    _cols = vars_select(_data.columns.tolist(), _cols, base0=base0_)

    series = [_data.iloc[:, col] for col in _cols]
    return numpy.concatenate(series)


@register_func(
    context=None,
    extra_contexts={"args": Context.SELECT},
    verb_arg_only=True,
    summarise_prefers_input=True,
)
def if_any(
    _data: DataFrame,
    *args: Any,
    _names: str = None,
    _context: ContextBase = None,
    base0_: bool = None,
    **kwargs: Any,
) -> Iterable[bool]:
    """Apply the same predicate function to a selection of columns and combine
    the results True if any element is True.

    See Also:
        [`across()`](datar.dplyr.across.across)
    """
    if not args:
        args = (None, None)
    elif len(args) == 1:
        args = (args[0], None)
    _cols, _fns, *args = args

    return IfAny(_data, _cols, _fns, _names, base0_, args, kwargs).evaluate(
        _context
    )


@register_func(
    context=None,
    extra_contexts={"args": Context.SELECT},
    verb_arg_only=True,
    summarise_prefers_input=True,
)
def if_all(
    _data: DataFrame,
    # _cols: Iterable[str] = None,
    # _fns: Union[Mapping[str, Callable]] = None,
    *args: Any,
    _names: str = None,
    _context: ContextBase = None,
    base0_: bool = None,
    **kwargs: Any,
) -> Iterable[bool]:
    """Apply the same predicate function to a selection of columns and combine
    the results True if all elements are True.

    See Also:
        [`across()`](datar.dplyr.across.across)
    """
    if not args:
        args = (None, None)
    elif len(args) == 1:
        args = (args[0], None)
    _cols, _fns, *args = args

    return IfAll(_data, _cols, _fns, _names, base0_, args, kwargs).evaluate(
        _context
    )
