import inspect
from collections import namedtuple
from functools import singledispatch
from typing import TYPE_CHECKING, Sequence

from pandas import DataFrame, Series
from pandas.api.types import is_categorical_dtype, is_scalar
from pandas.core.base import PandasObject
from pandas.core.groupby import SeriesGroupBy
from pipda import register_func, register_verb

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
    from inspect import Signature

NO_DEFAULT = object()

MetaInfo = namedtuple(
    "MetaInfo",
    [
        # The pre/post hook to handle arguments before running
        # apply/agg/transform
        "pre",
        "post",
        # The function to run apply/agg/transform
        "func",
    ],
)


def _preprocess_args(sign: "Signature", data_args, args, kwargs):
    bound = sign.bind(*args, **kwargs)
    bound.apply_defaults()
    if is_scalar(data_args):
        data_args = {data_args}

    values = []
    to_expand = {}
    for arg in data_args:
        if arg in sign.parameters:
            if (
                sign.parameters[arg].kind
                == sign.parameters[arg].VAR_POSITIONAL
            ):
                to_expand[arg] = [
                    f"{arg}[{i}]" for i, _ in enumerate(bound.arguments[arg])
                ]
                values.extend(bound.arguments[arg])
            else:
                values.append(bound.arguments[arg])
        elif "[" in arg:
            vararg, idx = arg[:-1].split("[", 1)
            values.append(bound.arguments[vararg][int(idx)])
        else:
            raise ValueError(f"Data argument doesn't exist: {arg}.")

    if not to_expand:
        names = data_args
    else:
        names = []
        for darg in data_args:
            if darg in to_expand:
                names.extend(to_expand[darg])
            else:
                names.append(darg)

    args_df = Tibble.from_pairs(names, values, _name_repair="minimal")
    args_raw = bound.arguments.copy()
    for arg in bound.arguments:
        if arg == "__args_frame":
            bound.arguments[arg] = args_df
        elif arg == "__args_raw":
            bound.arguments[arg] = args_raw
        elif arg in args_df or args_df.columns.str.startswith(f"{arg}$").any():
            bound.arguments[arg] = args_df[arg]
        elif sign.parameters[arg].kind == sign.parameters[arg].VAR_POSITIONAL:
            star_args = bound.arguments[arg]
            bound.arguments[arg] = tuple(
                args_df[f"{arg}[{i}]"] if f"{arg}[{i}]" in args_df else sarg
                for i, sarg in enumerate(star_args)
            )

    return bound


def _run_pandas(kind, func, x, args, kwargs, axis=None, apply_type=None):
    method = getattr(x, kind)
    if axis is None:
        return method(func, *args, **kwargs)
    if kind == "apply":
        return method(func, axis, result_type=apply_type, args=args, **kwargs)

    return method(func, axis, *args, **kwargs)


def _register_factory(dispatched, initial_func):
    def _register(types, func=NO_DEFAULT, pre=None, post=None, meta=True):
        if func is NO_DEFAULT:
            return lambda fun=None: _register(
                types, func=fun, pre=pre, post=post, meta=meta
            )

        if not meta and (pre or post):
            raise ValueError(
                "If not registering meta, no `pre` or `post` hook "
                "should be specified."
            )

        if not isinstance(types, Sequence):
            types = [types]

        for type_ in types:
            if meta:
                dispatched.meta.register(type_)(
                    MetaInfo(pre, post, func or initial_func)
                )
            elif func is None:
                raise ValueError("No function to register.")
            else:
                dispatched.register(type_)(func)

    return _register


def _apply_meta(metainfo: MetaInfo, qualname, run, x, args, kwargs):
    if metainfo.pre:
        arguments = metainfo.pre(x, *args, **kwargs)
        if arguments is not None:
            x, args, kwargs = arguments

    try:
        out = run(x, args, kwargs)
    except Exception as exc:
        raise ValueError(f"{exc} (registered function: {qualname})") from exc

    if metainfo.post:
        return metainfo.post(out, x, *args, **kwargs)

    return out


def dispatching(func=None, kind=None, qualname=None, apply_type=None):
    if func is None:
        return lambda fn: dispatching(
            func=fn, kind=kind, qualname=qualname, apply_type=apply_type
        )

    DEFAULT_META = MetaInfo(None, None, None)

    @singledispatch
    def _dispatched(__x, *args, **kwargs):
        # Series if Series not registered
        if is_categorical_dtype(__x):
            metainfo = _dispatched.meta.dispatch(SeriesCategorical)
        else:
            metainfo = _dispatched.meta.dispatch(__x.__class__)

        fun = metainfo.func or func
        if isinstance(fun, str):
            run_func = lambda x, a, kw: getattr(x, fun)(*a, **kw)
        else:
            run_func = lambda x, a, kw: fun(x, *a, **kw)

        return _apply_meta(
            metainfo,
            qualname,
            run_func,
            __x,
            args,
            kwargs,
        )

    @_dispatched.register(SeriesGroupBy)
    def _(__x, *args, **kwargs):
        if getattr(__x, "is_rowwise", False):
            metainfo = _dispatched.meta.dispatch(SeriesRowwise)
        else:
            metainfo = _dispatched.meta.dispatch(__x.__class__)

        out = _apply_meta(
            metainfo,
            qualname,
            lambda x, a, kw: _run_pandas(
                kind, metainfo.func or func, x, a, kw
            ),
            __x,
            args,
            kwargs,
        )
        if kind == "transform" and not metainfo.post:
            out = out.groupby(__x.grouper)
            if getattr(__x, "is_rowwise", False):
                out.is_rowwise = True
        return out

    @_dispatched.register(DataFrame)
    def _(__x, *args, **kwargs):
        metainfo = _dispatched.meta.dispatch(__x.__class__)
        return _apply_meta(
            metainfo,
            qualname,
            lambda x, a, kw: _run_pandas(
                kind,
                metainfo.func or func,
                x,
                a,
                kw,
                axis=0,
                apply_type=apply_type,
            ),
            __x,
            args,
            kwargs,
        )

    @_dispatched.register(TibbleGrouped)
    def _(__x, *args, **kwargs):
        metainfo = _dispatched.meta.dispatch(__x.__class__)
        out = _apply_meta(
            metainfo,
            qualname,
            lambda x, a, kw: _run_pandas(
                kind,
                metainfo.func or func,
                x._datar["grouped"],
                a,
                kw,
            ),
            __x,
            args,
            kwargs,
        )
        if kind == "transform" and not metainfo.post:
            return reconstruct_tibble(__x, out)
        return out

    @_dispatched.register(TibbleRowwise)
    def _(__x, *args, **kwargs):
        metainfo = _dispatched.meta.dispatch(__x.__class__)
        out = _apply_meta(
            metainfo,
            qualname,
            lambda x, a, kw: _run_pandas(
                kind,
                metainfo.func or func,
                x,
                a,
                kw,
                axis=1,
                apply_type=apply_type,
            ),
            __x,
            args,
            kwargs,
        )
        if kind == "transform" and not metainfo.post:
            return reconstruct_tibble(__x, out)
        return out

    _dispatched.meta = singledispatch(DEFAULT_META)
    return _dispatched


def func_factory(
    kind,  # transform, agg/aggregate, apply
    data_args,
    name=None,
    qualname=None,
    doc=None,
    apply_type=None,
    context=Context.EVAL,
    keep_series=False,
    signature=None,
    func=None,
):
    """Register non-data/contextual functions

    Args:
        kind: what kind of operation we should do on higher-order data structure
            (for example, `DataFrame` compared to `Series`)
            using the base function we registered.
        data_args: The arguments of the base function to be vectorized and
            recycled.
            The values of those arguments will compose a frame,
            which can be accessed by `__args_frame` argument in your
            base function. Since those values become `Series` objects,
            the raw values can be accessed by `__args_raw`.
        name: and
        qualname: and
        doc: The `__name__`, `__qualname__` and `__doc__` for the final
            returned function
        apply_type: The result type when we do apply on frames,
            only for kind `apply`. See the doc of `DataFrame.apply()`
        context: The context to evaluate the function
        signature: The function signature to detect data arguments from the
            function arguments. Default is `inspect.signature(func)`.
            But inspect cannot detect signature of some functions,
            for example, `numpy.sqrt()`, then you can pass a signature instead.
        keep_series: Whether try to keep the result as series
            if input is not series.
        func: The base function. The default data type to handle is `Series`.
            When hight-order data is encountered, for example,
            `SeriesGroupBy`, with `kind` `agg`, `sgb.agg(func)` will run for it.

    Returns:
        A pipda registered function
    """
    kind = arg_match(
        kind, "kind", ["transform", "apply", "agg", "aggregate", None]
    )
    # kind = arg_match(kind, "kind", ["transform", "agg", "aggregate"])

    if func is None:
        # work as a decorator
        return lambda fun: func_factory(
            kind=kind,
            data_args=data_args,
            name=name,
            qualname=qualname,
            doc=doc,
            apply_type=apply_type,
            context=context,
            func=fun,
        )

    funcname = name or func.__name__
    qualname = qualname or func.__qualname__

    if kind is None:
        dispatched = func
    else:
        dispatched = dispatching(func, kind, qualname, apply_type)

    sign = signature or inspect.signature(func)

    def _pipda_func(__x, *args, **kwargs):
        bound = _preprocess_args(sign, data_args, (__x, *args), kwargs)
        out = dispatched(*bound.args, **bound.kwargs)
        if (
            kind == "transform"
            and not isinstance(__x, PandasObject)
            and not keep_series
            and isinstance(out, Series)
        ):
            return out.values

        return out

    _pipda_func.__name__ = funcname
    _pipda_func.__qualname__ = qualname
    _pipda_func.__doc__ = doc or func.__doc__
    _pipda_func.dispatched = dispatched
    _pipda_func.register = _register_factory(dispatched, func)
    _pipda_func.__raw__ = func

    return register_func(None, context=context, func=_pipda_func)


context_func_factory = register_func
verb_factory = register_verb
