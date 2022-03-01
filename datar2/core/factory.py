from functools import singledispatch
from typing import Sequence

import numpy as np
from pandas import DataFrame, Series
from pandas.api.types import is_scalar, is_categorical_dtype
from pandas.core.generic import NDFrame
from pandas.core.groupby import SeriesGroupBy, GroupBy

from pipda import register_func

from .contexts import Context
from .tibble import SeriesCategorical, TibbleGrouped, SeriesRowwise, TibbleRowwise
from .utils import arg_match

NO_DEFAULT = object()


def _register_factory(proxy):
    def _register(
        types,
        func=NO_DEFAULT,
        pre=None,
        post=None,
        replace=False,
        stof=True
    ):
        if func is NO_DEFAULT:
            return lambda fun=None: _register(
                types, func=fun, pre=pre, post=post, replace=replace, stof=stof
            )

        if replace and (pre or post):
            raise ValueError(
                "If `replace` is true, no `pre` or `post` hook "
                "should be specified."
            )

        if not isinstance(types, Sequence):
            types = [types]

        for type_ in types:
            proxy.register(type_)(
                {
                    "pre": pre,
                    "post": post,
                    "replace": replace,
                    "func": func,
                    "stof": stof,
                }
            )

    return _register


def _attach_rowwise(out, x):
    out = out.groupby(x.grouper)
    setattr(out, "is_rowwise", getattr(x, "is_rowwise", False))
    return out


def _run(
    kind, x, args, kwargs, func, reginfo, post=None, axis=None, result_type=None
):
    if reginfo["replace"]:
        return reginfo["func"](x, *args, **kwargs)

    if reginfo["pre"]:
        preproc = reginfo["pre"](x, *args, **kwargs)
        if preproc is not None:
            x, args, kwargs = preproc

    run_fun = getattr(x, kind)
    func = reginfo["func"] or func
    if kind != "apply":
        if axis is None:
            out = run_fun(func, *args, **kwargs)
        else:
            out = run_fun(func, axis, *args, **kwargs)
    else:
        if axis is None:
            out = run_fun(func, *args, **kwargs)
        else:
            out = run_fun(
                func,
                axis,
                result_type=result_type,
                args=args,
                **kwargs,
            )

    if reginfo["post"]:
        return reginfo["post"](out, x, *args, **kwargs)

    if callable(post):
        return post(out, x)

    return out


def _transform_dispatched(func=None, is_vectorized=True, **vec_kwargs):
    if func is None:
        return lambda fun: _transform_dispatched(
            func=fun, is_vectorized=is_vectorized, **vec_kwargs
        )

    if not is_vectorized:
        vec_func = np.vectorize(func, **vec_kwargs)
    else:
        vec_func = func

    @singledispatch
    def _dispatched(x, *args, **kwargs):
        if is_scalar(x):
            return func(x, *args, **kwargs)
        # list, tuple, np.ndarray, etc
        return vec_func(x, *args, **kwargs)

    @_dispatched.register(NDFrame)
    def _(x: NDFrame, *args, **kwargs):
        if is_categorical_dtype(x):
            reginfo = _dispatched.proxy.dispatch(SeriesCategorical)
        else:
            reginfo = _dispatched.proxy.dispatch(x.__class__)

        return _run(
            "transform",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=0,
        )

    @_dispatched.register(GroupBy)
    def _(x: GroupBy, *args, **kwargs):
        if getattr(x, "is_rowwise", False):
            reginfo = _dispatched.proxy.dispatch(SeriesRowwise)
        else:
            reginfo = _dispatched.proxy.dispatch(x.__class__)

        return _run(
            "transform",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            post=_attach_rowwise,
        )

    @_dispatched.register(TibbleGrouped)
    def _(x: TibbleGrouped, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "transform",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=0,
            post=lambda out, x: out.regroup(),
        )

    _dispatched.proxy = singledispatch({
        "pre": None, "post": None, "replace": False, "func": None
    })
    return _dispatched


def _apply_dispatched(func=None, result_type=None, stof=True):
    if func is None:
        return lambda fun: _apply_dispatched(func=fun, result_type=result_type)

    DEFAULT_META = {
        "pre": None, "post": None, "replace": False, "func": None, "stof": stof
    }

    @singledispatch
    def _dispatched(x, *args, **kwargs):
        # list, tuple, np.ndarray, etc
        return func(x, *args, **kwargs)

    @_dispatched.register(Series)
    def _(x: Series, *args, **kwargs):
        # treat the series as a whole instead of element one by one
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        if reginfo["stof"] is not None:
            stof = reginfo["stof"]

        if stof:
            return _run(
                "apply",
                x.to_frame(),
                args,
                kwargs,
                func=func,
                reginfo=reginfo,
                axis=0,
                result_type=result_type,
            ).iloc[:, 0]

        return _run(
            "apply",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=0,
            result_type=result_type,
        )

    @_dispatched.register(DataFrame)
    def _(x: DataFrame, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "apply",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=0,
            result_type=result_type,
        )

    @_dispatched.register(GroupBy)
    def _(x: GroupBy, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "apply",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
        )

    @_dispatched.register(SeriesRowwise)
    def _(x: SeriesRowwise, *args, **kwargs):
        """Aggregation on SeriesRowwise object does nothing"""
        reginfo = _dispatched.proxy.dispatch(SeriesRowwise)
        reginfo_sgb = _dispatched.proxy.dispatch(SeriesGroupBy)
        if reginfo is reginfo_sgb or reginfo is DEFAULT_META:
            # not specifically registered for SeriesRowwise
            raise ValueError(
                f"`{_dispatched.proxy.__name__}` is not registered for "
                "rowwise variable."
            )

        return _run(
            "apply",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
        )

    @_dispatched.register(TibbleGrouped)
    def _(x: TibbleGrouped, *args, **kwargs):
        return _dispatched(x._datar["grouped"], *args, **kwargs)

    @_dispatched.register(TibbleRowwise)
    def _(x: TibbleRowwise, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "apply",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=1,
            result_type=result_type,
        )

    _dispatched.proxy = singledispatch(DEFAULT_META)
    return _dispatched


def _agg_dispatched(func=None, stof=True):
    if func is None:
        return lambda fun: _agg_dispatched(func=fun)

    DEFAULT_META = {
        "pre": None, "post": None, "replace": False, "func": None, "stof": stof
    }

    @singledispatch
    def _dispatched(x, *args, **kwargs):
        # list, tuple, np.ndarray, etc
        return func(x, *args, **kwargs)

    @_dispatched.register(DataFrame)
    def _(x: DataFrame, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "agg",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=0,
        )

    @_dispatched.register(Series)
    def _(x: Series, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        if reginfo["stof"] is not None:
            stof = reginfo["stof"]

        out = _run(
            "agg",
            x.to_frame() if stof else x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=0,
        )
        return out[0] if stof else out

    @_dispatched.register(GroupBy)
    def _(x: GroupBy, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "agg",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
        )

    @_dispatched.register(SeriesRowwise)
    def _(x: SeriesRowwise, *args, **kwargs):
        """Aggregation on SeriesRowwise object does nothing"""
        reginfo = _dispatched.proxy.dispatch(SeriesRowwise)
        reginfo_sgb = _dispatched.proxy.dispatch(SeriesGroupBy)
        if reginfo is reginfo_sgb or reginfo is DEFAULT_META:
            # not specifically registered for SeriesRowwise
            return x.obj.copy().groupby(x.grouper)

        return _run(
            "agg",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
        )

    @_dispatched.register(TibbleGrouped)
    def _(x: TibbleGrouped, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "agg",
            x._datar["grouped"],
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=0,
        )

    @_dispatched.register(TibbleRowwise)
    def _(x: TibbleRowwise, *args, **kwargs):
        reginfo = _dispatched.proxy.dispatch(x.__class__)
        return _run(
            "agg",
            x,
            args,
            kwargs,
            func=func,
            reginfo=reginfo,
            axis=1,
        )

    _dispatched.proxy = singledispatch(DEFAULT_META)
    return _dispatched


def func_factory(
    kind,  # transform, agg/aggregate, apply
    is_vectorized=True,
    name=None,
    doc=None,
    func=None,
    result_type=None,
    stof=True,
    **vec_kwargs,
):
    """Factory for functions without data as first argument.

    This function is used to initiate a function that used on different types
    of data. For example:
    >>> @func_factory(kind="transform")
    >>> def cubic(x):
    >>>     return x ** 3

    If no further registration, then the default behavior for scalar and
    np.ndarray will be handled by the function directly.

    For Series/DataFrame object, the result of `mean(f.x)` will be
    `x.transform(cubic)`

    For SeriesGroupBy object, the result will be
    `x.obj.transform(cubic).groupby(x.grouper)`

    For TibbleGrouped object, the result will be `x.transform(cubic).regroup()`

    The behavior for each type can be customized by, for example, for Series
    object:
    >>> cubic.register(Series, lambda x: x ** 3)

    Similar for aggregate and apply.

    Args:
        kind: Either "transform", or "agg/aggregate"
        # kind: Either "transform", "apply" or "agg/aggregate"
        is_vectorized: Whether the function is vectorized, only for transform.
        name: The name of the function returned.
            If not provided, will be `func.__name__`
        doc: Docstring of the function returned.
            If not provided, will be `func.__doc__`
        func: The function to work on scalars, sequences and np.ndarrays
            If not provided, this function will return a decorator
        result_type: The result type for apply.
            See more defaults on pandas DataFrame.apply's doc
        stof: Whether convert the series to frame and then do
            apply or agg/aggregate
            >>> s = Series([1, 2, 3])
            >>> s.agg(np.sum)  # 6
            >>> # however, when we wrap it:
            >>> out = s.agg(lambda x: np.sum(x))
            >>> # out == s
            >>> # but we expect 6
            >>> # to do it:
            >>> s.to_frame().agg(lambda x: np.sum(x))[0]

    Returns:
        If func is not provided, a decorator to decorate a function. Otherwise
        a function registered by
        `pipda.register_func(None, context=Context.EVAL)`
    """
    kind = arg_match(kind, "kind", ["transform", "apply", "agg", "aggregate"])
    # kind = arg_match(kind, "kind", ["transform", "agg", "aggregate"])

    if func is None:
        # work as a decorator
        return lambda fun: func_factory(
            kind=kind,
            is_vectorized=is_vectorized,
            name=name,
            doc=doc,
            func=fun,
            **vec_kwargs,
        )

    dispatched = (
        _transform_dispatched(func, is_vectorized, **vec_kwargs)
        if kind == "transform"
        else _apply_dispatched(func, result_type, stof)
        if kind == "apply"
        else _agg_dispatched(func, stof)
    )

    @register_func(None, context=Context.EVAL)
    def pipda_func(x, *args, **kwargs):
        if isinstance(x, SeriesGroupBy) and getattr(x, "is_rowwise", False):
            return dispatched.dispatch(SeriesRowwise)(x, *args, **kwargs)

        return dispatched(x, *args, **kwargs)

    pipda_func.__name__ = dispatched.proxy.__name__ = name or func.__name__
    pipda_func.__doc__ = doc or func.__doc__
    pipda_func.register = _register_factory(dispatched.proxy)
    pipda_func.__raw__ = func

    return pipda_func
