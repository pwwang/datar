"""Apply a function (or functions) across multiple columns

See source https://github.com/tidyverse/dplyr/blob/master/R/across.R
"""
from abc import ABC, abstractmethod

from pipda import register_verb, evaluate_expr, Verb

from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.api.types import is_scalar

from ..core.broadcast import add_to_tibble
from ..core.tibble import Tibble, reconstruct_tibble
from ..core.utils import vars_select
from ..core.middlewares import CurColumn
from ..core.contexts import Context
from .tidyselect import everything


class Across:
    """Across object"""

    def __init__(
        self,
        data,
        cols=None,
        fns=None,
        names=None,
        args=None,
        kwargs=None,
    ):
        cols = (
            data >> everything()
            if cols is None
            else cols
        )
        if is_scalar(cols):
            cols = [cols]

        cols = data.columns[vars_select(data.columns, cols)]

        fns_list = []
        if callable(fns):
            fns_list.append({"fn": fns})
        elif isinstance(fns, (list, tuple)):
            fns_list.extend(
                {"fn": fn, "_fn": i, "_fn1": i + 1, "_fn0": i}
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

    def evaluate(self, context=None):
        """Evaluate object with context"""
        if isinstance(context, Context):
            context = context.value

        if not self.fns:
            self.fns = [{"fn": lambda x: x}]

        ret = None
        # Instead of df.apply(), we can recycle groupby values and more
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

                if isinstance(fn, Verb):
                    if fn.registered(DataFrame):
                        value = fn(
                            self.data,
                            self.data[column],
                            *args,
                            __ast_fallback="normal",
                            **kwargs,
                        )
                    else:
                        value = fn(
                            self.data[column],
                            *args,
                            __ast_fallback="normal",
                            **kwargs,
                        )
                else:
                    value = fn(
                        self.data[column],
                        *evaluate_expr(args, self.data, context),
                        **evaluate_expr(kwargs, self.data, context),
                    )

                ret = add_to_tibble(ret, name, value, broadcast_tbl=True)

        return Tibble() if ret is None else ret


class IfCross(Across, ABC):
    """Base class for IfAny and IfAll"""

    @staticmethod
    @abstractmethod
    def aggregate(values):
        """How to aggregation by rows"""

    def evaluate(
        self,
        context=None,
    ):
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
    def aggregate(values):
        """How to aggregation by rows"""
        return values.fillna(False).astype(bool).any()


class IfAll(IfCross):
    """For calls from dplyr's if_all"""

    @staticmethod
    def aggregate(values):
        """How to aggregation by rows"""
        return values.fillna(False).astype(bool).all()


@register_verb(
    DataFrame,
    context=Context.PENDING,
    dep=True,
)
def across(
    _data,
    *args,
    _names=None,
    _fn_context=Context.EVAL,
    **kwargs,
):
    """Apply the same transformation to multiple columns

    The original API:
    https://dplyr.tidyverse.org/reference/across.html

    Examples:
        #
        >>> iris >> mutate(across(c(f.Sepal_Length, f.Sepal_Width), round))
            Sepal_Length  Sepal_Width  Petal_Length  Petal_Width    Species
               <float64>    <float64>     <float64>    <float64>   <object>
        0            5.0          4.0           1.4          0.2     setosa
        1            5.0          3.0           1.4          0.2     setosa
        ..           ...          ...           ...          ...        ...

        >>> iris >> group_by(f.Species) >> summarise(
        >>>     across(starts_with("Sepal"), mean)
        >>> )
              Species  Sepal_Length  Sepal_Width
             <object>     <float64>    <float64>
        0      setosa         5.006        3.428
        1  versicolor         5.936        2.770
        2   virginica         6.588        2.974

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
        _fn_context: Defines the context to evaluate the arguments for functions
            if they are plain functions.
            Note that registered functions will use its own context
        **kwargs: Keyword arguments for the functions

    Returns:
        A dataframe with one column for each column and each function.
    """
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)

    if not args:
        args = (None, None)
    elif len(args) == 1:
        args = (args[0], None)
    _cols, _fns, *args = args
    _cols = evaluate_expr(_cols, _data, Context.SELECT)

    return Across(
        _data,
        _cols,
        _fns,
        _names,
        args,
        kwargs,
    ).evaluate(_fn_context)


@register_verb(DataFrame, context=Context.SELECT, dep=True)
def c_across(
    _data,
    _cols=None,
):
    """Apply the same transformation to multiple columns rowwisely

    Args:
        _data: The dataframe
        _cols: The columns

    Returns:
        A rowwise tibble
    """
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)

    if not _cols:
        _cols = _data >> everything()

    _cols = vars_select(_data.columns, _cols)
    return reconstruct_tibble(_data, _data.iloc[:, _cols])


@register_verb(
    DataFrame,
    context=None,
    extra_contexts={"args": Context.SELECT},
    dep=True,
)
def if_any(
    _data,
    *args,
    _names=None,
    _context=None,
    **kwargs,
):
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
    _data = getattr(_data, "_datar", {}).get("summarise_source", _data)

    return IfAny(
        _data,
        _cols,
        _fns,
        _names,
        args,
        kwargs,
    ).evaluate(_context)


@register_verb(
    DataFrame,
    context=None,
    extra_contexts={"args": Context.SELECT},
    dep=True,
)
def if_all(
    _data,
    # _cols: Iterable[str] = None,
    # _fns: Union[Mapping[str, Callable]] = None,
    *args,
    _names=None,
    _context=None,
    **kwargs,
):
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
    _data = getattr(_data, "_datar", {}).get("summerise_source", _data)

    return IfAll(
        _data,
        _cols,
        _fns,
        _names,
        args,
        kwargs,
    ).evaluate(_context)
