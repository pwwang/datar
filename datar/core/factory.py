"""Provides functions to register functions, these functions are dealing with

"""
from __future__ import annotations

import inspect
from abc import ABC
from functools import singledispatch
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    Set,
    Callable,
    Tuple,
    Type,
)

from pipda import Function, FunctionCall, evaluate_expr
from pipda.utils import has_expr

from .backends.pandas import DataFrame, Series
from .backends.pandas.api.types import is_categorical_dtype, is_scalar
from .backends.pandas.core.base import PandasObject

from .backends.pandas.core.groupby import SeriesGroupBy
from .contexts import Context
from .utils import arg_match
from .tibble import (
    SeriesCategorical,
    SeriesRowwise,
    Tibble,
    TibbleGrouped,
    TibbleRowwise,
    reconstruct_tibble,
)

if TYPE_CHECKING:
    from inspect import Signature, BoundArguments
    from pipda.context import ContextType


def _transform_seriesrowwise_post(__out, x, *args, **kwargs):
    out = __out.groupby(x.grouper)
    out.is_rowwise = True
    return out


def _df_to_dict(df: DataFrame) -> Mapping[str, DataFrame | Series]:
    """Convert a frame (with nested frames) into a dict."""
    has_nest = any("$" in str(col) for col in df.columns)
    if not has_nest:
        return df.to_dict("series")

    out = {}
    for col in df.columns:
        if '$' not in col:
            out[col] = df.loc[:, col]
        else:
            series = df.loc[:, col]
            outcol, subcol = col.split('$', 1)
            if outcol not in out:
                out[outcol] = series.to_frame(name=subcol)
            else:
                out[outcol][subcol] = series
    return out


def _series_to_dict(s: Series) -> Mapping[str, Any]:
    """Convert a frame (with nested frames) into a dict."""
    has_nest = any("$" in str(col) for col in s.index)
    if not has_nest:
        return s.to_dict()

    out = {}
    for col in s.index:
        if '$' not in col:
            out[col] = s[col]
        else:
            o = s[col]
            outcol, subcol = col.split('$', 1)
            if outcol not in out:
                out[outcol] = Series(o, index=[subcol])
            else:
                out[outcol][subcol] = o
    return out


class DatarFunctionCall(FunctionCall):
    """Call DatarFunction so we can have expressions in the arguments"""

    def _pipda_eval(self, data: Any, context: ContextType = None) -> Any:
        func = self._pipda_func
        bound = func.bind_arguments(*self._pipda_args, **self._pipda_kwargs)
        context = func.context or context
        for key, val in bound.arguments.items():
            ctx = func.extra_contexts.get(key, context)
            val = evaluate_expr(val, data, ctx)
            bound.arguments[key] = val

        return func.call_bound(bound)


class DatarFunction(Function, ABC):
    """The registered functions that can handle groupedness"""

    OPERATOR_FUN = None
    # op_args:
    #   Args in DataFrame.apply(func, axis, raw, result_type, args, **kwargs)
    #                                 ^^^^^^^^^^^^^^^^^^^^^^
    # keep_args:
    #   Whether to keep the `args` as a whole to pass to the operator or
    #   decouple them by `*args`
    #
    # op_kwargs:
    #   The keyword argument, will be passed to op by `**op_kwargs`
    DEFAULT_DISPATCHEE_META = {
        object: {
            "pre": None,
            "post": None,
            "op_args": (),
            "keep_args": False,
            "op_kwargs": {},
            "keep_series": False,
        },
        SeriesRowwise: {
            "pre": lambda x, *args, **kwargs: (x.obj, args, kwargs)
        },
        TibbleGrouped: {
            "pre": lambda x, *args, **kwargs: (
                x._datar["grouped"],
                args,
                kwargs,
            )
        },
    }

    def __init__(
        self,
        func: str | Callable,
        data_args: Set,
        context: ContextType,
        extra_contexts: Mapping[str, ContextType],
        name: str,
        qualname: str,
        doc: str,
        module: str,
        signature: Signature,
    ) -> None:
        self.context = context
        self.extra_contexts = extra_contexts
        self.data_args = data_args
        self._signature = signature or inspect.signature(func)

        self.default_func = func

        # update metadata
        # Can't do it with functools.update_wrapper()
        # numpy.ufunc and alike fails
        self.__name__ = name or func.__name__
        self.__qualname__ = qualname or func.__qualname__
        self.__module__ = module or func.__module__
        self.__doc__ = doc or func.__doc__

        self.func = singledispatch(lambda *args, **kwargs: ...)
        self.registry = self.func.registry
        self.dispatch = self.func.dispatch

        # Default dispatchers
        # You can still register them later, but these are registered
        # by default so that we can handle the corresponding data types
        # automatically
        self._register_defaults()

    def _preprocess_boundargs(self, bound: BoundArguments) -> Tibble:
        """Preprocess the arguments passed in.

        Broadcast the arguments from self.data_args to form __args_frame and
        keep raw arguments in __args_raw. If these two arguments are provided,
        attach the values to the function

        Args:
            bound: The bound arguments by `self.signature.bind_arguments(...)`

        Returns:
            The tibble with self.data_args as columns
        """
        diff_args = self.data_args - set(bound.arguments)
        if diff_args:
            raise ValueError(f"Data argument doesn't exist: {diff_args}.")

        if not self.data_args:
            self.data_args = {list(bound.signature.parameters)[0]}

        args_raw = bound.arguments.copy()
        args_df = Tibble.from_args(
            **{
                key: (
                    val
                    if bound.signature.parameters[key].kind
                    != inspect.Parameter.VAR_POSITIONAL
                    else Tibble.from_pairs(
                        [str(i) for i in range(len(val))],
                        val
                    )
                )
                for key, val in bound.arguments.items()
                if key in self.data_args
            }
        )
        # inject __args_raw and __args_frame
        for arg in bound.arguments:
            if arg == "__args_frame":
                bound.arguments[arg] = args_df
            elif arg == "__args_raw":
                bound.arguments[arg] = args_raw
            elif (
                arg in args_df
                or args_df.columns.str.startswith(f"{arg}$").any()
            ):
                # nest frames
                if (
                    bound.signature.parameters[arg].kind
                    != inspect.Parameter.VAR_POSITIONAL
                ):
                    bound.arguments[arg] = args_df[arg]
                else:
                    bound.arguments[arg] = tuple(
                        args_df[arg].to_dict("series").values()
                    )

        return args_df

    def _default_meta(
        self,
        cls: Type,
        key: str,
    ) -> Callable | Mapping[str, Any]:
        """Get the default meta for the type"""
        super_meta = DatarFunction.DEFAULT_DISPATCHEE_META
        my_meta = self.__class__.DEFAULT_DISPATCHEE_META
        base = super_meta[object].copy()
        if object in my_meta:
            base.update(my_meta[object])
        if cls in super_meta:
            base.update(super_meta[cls])
        if cls in my_meta:
            base.update(my_meta[cls])

        return base[key]

    def register_dispatchee(
        self,
        cls: Type,
        func: Callable = None,
    ) -> DatarFunction:
        """Register functions to deal with everything with the cls

        Args:
            cls: The type to be registered
            func: The function, include the pre/post processes
        """
        if not func:
            return lambda fun: self.register(cls, fun)

        self.func.register(cls, func)
        return self

    def register(
        self,
        cls: Type,
        pre: str | Callable = "default",
        post: str | Callable = "default",
        op_args: Tuple = None,
        keep_args: bool = None,
        op_kwargs: Mapping[str, Any] = None,
        keep_series: bool = None,
        func: str | Callable = None,
    ) -> DatarFunction:
        """Register the focal function, which will be applied to
        agg/transform/apply

        We could also have pre function to get prepared before
        agg/transform/apply and post function to wrap out the results to
        deliver.

        Other than being passed as arguments, pre/post functions can be also
        retrieved from `func.pre`/`func.post` if they are not passed.

        Args:
            cls: The cls to be registered
            pre: The pre function to prepare for agg/transform/apply
                It takes the arguments passed to the function and returns None
                or (x, args, kwargs) to replace the original
                If `"default"` use the default one
            post: The  post function to wrap up results
                It takes `__out, *args, **kwargs` and returns the output.
                If `"default"` use the default one
            op_args: The `(axis, raw, result_type)` alike inside
                `DataFrame.apply(func, axis, raw, result_type, args, **kwargs)`
            keep_args: Whether to keep the `args` as a whole to pass to the
                operator or decouple them by `*args`
            op_kwargs: The keyword argument, will be passed to operation
                by `**op_kwargs`
            keep_series: Whether to keep Series if input is not a PandasObject.
                Only for transform
            func: The focal function used in agg/transform/apply
                If func is "default"`, initial registered function is used.
        """
        if not func:
            return lambda fun: self.register(
                cls=cls,
                pre=pre,
                post=post,
                op_args=op_args,
                keep_args=keep_args,
                op_kwargs=op_kwargs,
                keep_series=keep_series,
                func=fun,
            )

        if op_args is None:
            op_args = self._default_meta(cls, "op_args")
        if keep_args is None:
            keep_args = self._default_meta(cls, "keep_args")
        if op_kwargs is None:
            op_kwargs = self._default_meta(cls, "op_kwargs")
        if keep_series is None:
            keep_series = self._default_meta(cls, "keep_series")

        if func == "default":
            # The inital function
            func = self.default_func
        if pre == "default":
            pre = self._default_meta(cls, "pre")
        if post == "default":
            post = self._default_meta(cls, "post")

        def dispatchee(x, *args, **kwargs):
            oldx = x
            if pre:
                arguments = pre(oldx, *args, **kwargs)
                if arguments is not None:
                    x, args, kwargs = arguments

            out = self.run(
                func,
                op_args,
                keep_args,
                op_kwargs,
                x,
                *args,
                **kwargs,
            )
            if post:
                out = post(out, oldx, *args, **kwargs)
            return out

        dispatchee.keep_series = keep_series
        return self.register_dispatchee(cls, func=dispatchee)

    def _register_defaults(self):
        """Register the default dispatchers"""
        for typ in (
            object,
            Series,
            SeriesGroupBy,
            SeriesRowwise,
            DataFrame,
            TibbleGrouped,
            TibbleRowwise,
        ):
            self.register(typ, func="default")

    def run(
        self,
        func: str | Callable,
        op_args: Mapping[str, Any],
        keep_args: bool,
        op_kwargs: Mapping[str, Any],
        x: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run agg/transform/apply"""
        op = self.__class__.OPERATOR_FUN
        fun = getattr(x, op)
        if keep_args:
            return fun(func, *op_args, args, **kwargs, **op_kwargs)

        return fun(func, *op_args, *args, **kwargs, **op_kwargs)

    def call_bound(self, bound: BoundArguments) -> Any:
        """Call with the bound and evaluated arguments"""
        orig_x = bound.args[0]
        self._preprocess_boundargs(bound)
        x, *args = bound.args
        if isinstance(x, SeriesGroupBy) and getattr(x, "is_rowwise", False):
            fun = self.dispatch(SeriesRowwise)
        elif isinstance(x, Series) and is_categorical_dtype(x):
            fun = self.dispatch(SeriesCategorical)
        else:
            fun = self.dispatch(type(orig_x))

        return fun(x, *args, **bound.kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the function"""
        if has_expr(args) or has_expr(kwargs):
            return DatarFunctionCall(self, *args, **kwargs)

        return self.call_bound(self.bind_arguments(*args, **kwargs))


class DatarTransformFunction(DatarFunction):
    """Automatically apply transform on first argument with given function"""

    OPERATOR_FUN = "transform"
    DEFAULT_DISPATCHEE_META = {
        object: {"op_args": (0,)},
        SeriesGroupBy: {
            "post": (
                lambda __out, x, *args, **kwargs: __out.groupby(x.grouper)
            ),
            "op_args": (),
        },
        SeriesRowwise: {
            "post": _transform_seriesrowwise_post,
            "op_args": (0,),
        },
        TibbleGrouped: {
            "post": (
                lambda __out, x, *args, **kwargs: reconstruct_tibble(x, __out)
            ),
            "op_args": (),
        },
        TibbleRowwise: {
            "post": (
                lambda __out, x, *args, **kwargs: reconstruct_tibble(x, __out)
            ),
            "op_args": (1,),
        },
    }

    def call_bound(self, bound: BoundArguments) -> Any:
        orig_x = bound.args[0]
        self._preprocess_boundargs(bound)
        x, *args = bound.args
        if isinstance(x, SeriesGroupBy) and getattr(x, "is_rowwise", False):
            fun = self.dispatch(SeriesRowwise)
        elif isinstance(x, Series) and is_categorical_dtype(x):
            fun = self.dispatch(SeriesCategorical)
        else:
            # orig_x could be a ndarray
            # but x is a Series then
            fun = self.dispatch(type(orig_x))

        out = fun(x, *args, **bound.kwargs)
        if (
            not isinstance(orig_x, PandasObject)
            and isinstance(out, Series)
            and not fun.keep_series
        ):
            return out.values

        return out


class DatarAggFunction(DatarFunction):
    """Automatically aggregation on first argument with given function"""

    OPERATOR_FUN = "agg"
    DEFAULT_DISPATCHEE_META = {
        object: {"op_args": (0,)},
        SeriesGroupBy: {"op_args": {}},
        SeriesRowwise: {"pre": None, "op_args": ()},
        TibbleGrouped: {"op_args": {}},
        TibbleRowwise: {"op_args": (1,)},
    }


class DatarApplyFunction(DatarFunction):
    """Automatically apply on first argument with given function"""

    OPERATOR_FUN = "apply"
    DEFAULT_DISPATCHEE_META = {
        object: {"keep_args": True},
        Series: {"op_args": (True,)},  # convert_dtype
        DataFrame: {"op_args": (0, False, None)},  # axis, raw, result_type
        TibbleGrouped: {"keep_args": False, "op_args": ()},
        TibbleRowwise: {"op_args": (1, False, None)},
    }


class DatarApplyDfFunction(DatarFunction):
    """Automatically apply on __args_frame with given function"""

    DEFAULT_DISPATCHEE_META = {
        TibbleGrouped: {
            "pre": None,
            "op_args": (),
            "op_kwargs": {},
        },
        TibbleRowwise: {
            "op_args": (1, False, None),
        }
    }

    def _register_defaults(self):
        for typ in (
            DataFrame,
            TibbleGrouped,
            TibbleRowwise,
        ):
            self.register(typ, func="default")

    def register(
        self,
        cls: Type,
        pre: str | Callable = "default",
        post: str | Callable = "default",
        op_args: Tuple = None,
        keep_args: bool = None,
        op_kwargs: Mapping[str, Any] = None,
        keep_series: bool = None,  # compatible with other function.register
        func: str | Callable = None,
    ) -> DatarFunction:
        """Register the focal function"""
        if not func:
            return lambda fun: self.register(
                cls=cls,
                pre=pre,
                post=post,
                op_args=op_args,
                keep_args=keep_args,
                op_kwargs=op_kwargs,
                keep_series=keep_series,
                func=fun,
            )

        if not issubclass(cls, DataFrame):
            raise TypeError(
                "`apply_df` kind of functions can only be registered "
                "for DataFrame"
            )

        if op_args is None:
            op_args = self._default_meta(cls, "op_args")
        if op_kwargs is None:
            op_kwargs = self._default_meta(cls, "op_kwargs")

        if func == "default":
            # The inital function
            func = self.default_func
        if pre == "default":
            pre = self._default_meta(cls, "pre")
        if post == "default":
            post = self._default_meta(cls, "post")

        def dispatchee(args_df, bound):
            if pre:
                arguments = pre(**bound.arguments)
                if arguments is not None:
                    bound.arguments.update(arguments)

            out = self.run(
                func,
                args_df,
                bound,
                op_args,
                op_kwargs,
            )
            if post:
                out = post(out, **bound.arguments)
            return out

        return self.register_dispatchee(cls, func=dispatchee)

    def run(
        self,
        func: str | Callable,
        args_df: Tibble,
        bound: BoundArguments,
        op_args: Mapping[str, Any],
        op_kwargs: Mapping[str, Any],
    ) -> Any:
        """Run agg/transform/apply"""
        if not isinstance(args_df, TibbleGrouped):
            # Tibble or DataFrame
            return func(*bound.args, **bound.kwargs)

        if not isinstance(args_df, TibbleRowwise):
            # TibbleGrouped
            args_df = args_df._datar["grouped"]
            to_dict = _df_to_dict
        else:
            to_dict = _series_to_dict

        def to_be_applied(subdf):
            # replace the bound.arguments with columns in subdf
            dic = to_dict(subdf)
            bound.arguments.update(dic)
            return func(*bound.args, **bound.kwargs)

        return args_df.apply(to_be_applied, *op_args, **op_kwargs)

    def call_bound(self, bound: BoundArguments) -> Any:
        """Call with the bound and evaluated arguments"""
        args_df = self._preprocess_boundargs(bound)
        fun = self.dispatch(type(args_df))  # dispatchee

        return fun(args_df, bound)


def func_factory(
    data_args: str | Set = None,
    kind: str = "apply_df",  # transform, agg/aggregate, apply, apply_df
    context: ContextType = Context.EVAL,
    extra_contexts: Mapping[str, ContextType] = None,
    name: str = None,
    qualname: str = None,
    doc: str = None,
    module: str = None,
    signature: Signature = None,
    func: str | Callable = None,
):
    """Register functions

    Note that we have to deal with the groupedness by ourselves, so the data
    could be grouped. There are builtin handlers for the groupedness, but you
    can handle it by yourself using singledispatch.

    The arguments listed in `data_args` passed to the registered function,
    will be broadcasted to each other and merged into a data frame, which can
    be retrieved by `__args_frame`. The raw arguments can still be retrieved
    by `__args_raw`.

    There are different ways to deal with the groupedness
    on the first argument `x`:
    - `agg/aggregate`: Run `x.agg()`
    - `transform`: Run `x.transform()`
    - `apply`: Run `x.apply()`
    - `None`: deal the groupedness all by yourself

    With `apply_df`, we are running `__args_frame.apply()` on multiple arguments
    assigned by `data_args`.

    Args:
        data_args: The arguments of the base function to be vectorized and
            recycled.
            The values of those arguments will compose a frame,
            which can be accessed by `__args_frame` argument in your
            base function. Since those values become `Series` objects,
            the raw values can be accessed by `__args_raw`.
            If not specified, the first argument is used
        kind: what kind of operation we should do on higher-order data structure
            (for example, `DataFrame` compared to `Series`)
            using the base function we registered.
        name: The name of the function if func.__name__ is unavailable
        qualname: The qualname of the function if func.__qualname__ is
            unavailable
        module: The module of the function if func.__module__ is unavailable
        doc: The doc of the function if func.__doc__ is unavailable
        signature: The signature of the function if it is unavailable
        context: The context to evaluate the function
        extra_contexts: The extra contexts used on keyword-only arguments
        func: Default focal function to use in agg/transform/apply

    Returns:
        A registered function (A DatarFunction object)
    """
    if func is None:
        # work as a decorator
        return lambda fun: func_factory(
            kind=kind,
            data_args=data_args,
            context=context,
            extra_contexts=extra_contexts,
            name=name,
            qualname=qualname,
            module=module,
            doc=doc,
            signature=signature,
            func=fun,
        )

    kind = arg_match(
        kind,
        "kind",
        ["transform", "apply", "agg", "aggregate", "apply_df"],
    )
    if data_args and is_scalar(data_args):
        data_args = {data_args}

    kwds = {
        "func": func,
        "data_args": data_args or set(),
        "context": context,
        "extra_contexts": extra_contexts or {},
        "name": name,
        "qualname": qualname,
        "doc": doc,
        "module": module,
        "signature": signature,
    }
    if kind in ("agg", "aggregation"):
        return DatarAggFunction(**kwds)
    if kind == "transform":
        return DatarTransformFunction(**kwds)
    if kind == "apply":
        return DatarApplyFunction(**kwds)

    # apply_df
    return DatarApplyDfFunction(**kwds)
