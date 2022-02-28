"""Core utilities"""
import sys
import logging
from functools import singledispatch
from typing import Any, Iterable, Mapping, Sequence, Union

import numpy as np
from pandas import DataFrame, Series
from pandas._typing import AnyArrayLike, FuncType
from pandas.api.types import is_scalar
from pandas.core.apply import SeriesApply
from pandas.core.groupby import SeriesGroupBy, GroupBy
from pipda import register_func
from pipda.utils import CallingEnvs

from .contexts import Context

# logger
logger = logging.getLogger("datar")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s][%(name)s][%(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(stream_handler)


@singledispatch
def name_of(value) -> str:
    return str(value)


@name_of.register
def _(value: Series) -> str:
    return value.name


@name_of.register
def _(value: SeriesGroupBy) -> str:
    return value.obj.name


@name_of.register
def _(value: DataFrame) -> str:
    return None


def regcall(func: FuncType, *args: Any, **kwargs: Any) -> Any:
    """Call function with regular calling env"""
    return func(*args, **kwargs, __calling_env=CallingEnvs.REGULAR)


def ensure_nparray(x: Any) -> np.ndarray:
    if is_scalar(x):
        return np.array(x).ravel()

    if isinstance(x, dict):
        return np.array(list(x))

    if not isinstance(x, np.ndarray):
        return np.array(x)

    return x


def arg_match(
    arg: Any, argname: str, values: Iterable, errmsg: str = None,
) -> Any:
    """Make sure arg is in one of the values.

    Mimics `rlang::arg_match`.
    """
    if not errmsg:
        values = list(values)
        errmsg = f"`{argname}` must be one of {values}."
    if arg not in values:
        raise ValueError(errmsg)
    return arg


def vars_select(
    all_columns: Iterable[str],
    *columns: Any,
    raise_nonexists: bool = True,
) -> Sequence[int]:
    # TODO: support selecting data-frame columns
    """Select columns

    Args:
        all_columns: The column pool to select
        *columns: arguments to select from the pool
        raise_nonexist: Whether raise exception when column not exists
            in the pool

    Returns:
        The selected indexes for columns

    Raises:
        KeyError: When the column does not exist in the pool
            and raise_nonexists is True.
    """
    from .collections import Collection
    from ..base import unique

    columns = [
        column.name
        if isinstance(column, Series)
        else column.obj.name
        if isinstance(column, SeriesGroupBy)
        else column
        for column in columns
    ]
    selected = Collection(*columns, pool=list(all_columns))
    if raise_nonexists and selected.unmatched and selected.unmatched != {None}:
        raise KeyError(
            f"Columns `{selected.unmatched}` do not exist."
        )
    return regcall(unique, selected).astype(int)


def transform_func(
    name: str,
    doc: str,
    transform: Union[str, FuncType] = None,
    vectorized: bool = True,
    vec_kwargs: Mapping[str, Any] = None
) -> FuncType:
    """Register transform functions applied to each element of the input

    Args:
        name: The name of the function, will be written to `fun.__name__` of
            the function returned
        doc: The doc of the function, will be written to `fun.__doc__` of the
            function returned
        transform: Function name from `numpy` to be used for transformation.
            Or function to apply to each element
        vectorized: Whether the function is vectorized
    """
    if transform is None:
        transform = name

    if isinstance(transform, str):
        np_func = getattr(np, transform)
    elif not vectorized:
        if not vec_kwargs:
            vec_kwargs = {}
        np_func = np.vectorize(transform, **vec_kwargs)
    else:
        np_func = transform

    @singledispatch
    def _disp_func(x, *args, **kwargs):
        """Plain scalar/arrays"""
        return np_func(x, *args, **kwargs)

    @_disp_func.register(Series)
    def _(x: Series, *args, **kwargs):
        # trick x.transform, because
        # x.transform(np_func, 0, *args, **kwargs)
        # x.transform(np_func, *args, axis=0, **kwargs)
        # raise errors
        x._get_axis_number(0)
        return SeriesApply(
            x, func=np_func, convert_dtype=True, args=args, kwargs=kwargs
        ).transform()


    @_disp_func.register(GroupBy)
    def _(x: GroupBy, *args, **kwargs):
        out = _disp_func(x.obj, *args, **kwargs).groupby(x.grouper)
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

    @register_func(None, context=Context.EVAL)
    def _func(x, *args, **kwargs) -> AnyArrayLike:
        return _disp_func(x, *args, **kwargs)

    _func.__name__ = name
    _func.__doc__ = doc
    return _func


def transform_func_decor(vectorized: bool = True, **vec_kwargs) -> FuncType:
    """A decorator verions of transform_func"""
    return lambda func: transform_func(
        func.__name__,
        func.__doc__,
        func,
        vectorized=vectorized,
        vec_kwargs=vec_kwargs,
    )
