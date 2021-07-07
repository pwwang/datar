"""Some functions from R-base

If a function uses DataFrame/DataFrameGroupBy as first argument, it may be
registered by `register_verb` and should be placed in `./verbs.py`
"""
import itertools
from typing import Any, Iterable

import pandas
from pandas import Categorical, DataFrame
from pipda import register_func

from ..core.middlewares import WithDataEnv
from ..core.types import NumericType
from ..core.contexts import Context


@register_func(None, context=Context.EVAL)
def cut(
    x: Iterable[NumericType],
    breaks: Any,
    labels: Iterable[Any] = None,
    include_lowest: bool = False,
    right: bool = True,
    precision: int = 2,
    ordered_result: bool = False,
) -> Categorical:
    """Divides the range of x into intervals and codes the values in x
    according to which interval they fall. The leftmost interval corresponds
    to level one, the next leftmost to level two and so on.

    Args:
        x: a numeric vector which is to be converted to a factor by cutting.
        breaks: either a numeric vector of two or more unique cut points or
            a single number (greater than or equal to 2) giving the number of
            intervals into which x is to be cut.
        labels: labels for the levels of the resulting category. By default,
            labels are constructed using "(a,b]" interval notation.
            If labels = False, simple integer codes are returned instead
            of a factor.
        include_lowest: bool, indicating if an ‘x[i]’ equal to the lowest
            (or highest, for right = FALSE) ‘breaks’ value should be included.
        right: bool, indicating if the intervals should be closed on the right
            (and open on the left) or vice versa.
        precision:integer which is used when labels are not given. It determines
            the precision used in formatting the break numbers. Note, this
            argument is different from R's API, which is dig.lab.
        ordered_result: bool, should the result be an ordered categorical?

    Returns:
        A categorical object with the cuts
    """
    if labels is None:
        ordered_result = True

    return pandas.cut(
        x,
        breaks,
        labels=labels,
        include_lowest=include_lowest,
        right=right,
        precision=precision,
        ordered=ordered_result,
    )


@register_func(None, context=Context.EVAL)
def identity(x: Any) -> Any:
    """Return whatever passed in

    Expression objects are evaluated using parent context
    """
    return x


@register_func(None, context=Context.EVAL)
def expandgrid(*args: Iterable[Any], **kwargs: Iterable[Any]) -> DataFrame:
    """Expand all combinations into a dataframe. R's `expand.grid()`"""
    iters = {}
    for i, arg in enumerate(args):
        name = getattr(arg, "name", getattr(arg, "__name__", f"Var{i}"))
        iters[name] = arg
    iters.update(kwargs)

    return DataFrame(
        list(itertools.product(*iters.values())), columns=iters.keys()
    )


# ---------------------------------
# Plain functions
# ---------------------------------


def data_context(data: DataFrame) -> Any:
    """Evaluate verbs, functions in the
    possibly modifying (a copy of) the original data.

    It mimic the `with` function in R, but you have to write it in a python way,
    which is using the `with` statement. And you have to use it with `as`, since
    we need the value returned by `__enter__`.

    Args:
        data: The data
        func: A function that is registered by
            `pipda.register_verb` or `pipda.register_func`.
        *args: Arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        The original or modified data
    """
    return WithDataEnv(data)
