from functools import singledispatch
from typing import Any, Sequence, Iterable, Union

import numpy as np
import pandas as pd
from pandas import Series
from pandas.api.types import is_scalar, is_integer
from pandas.core.groupby import SeriesGroupBy, GroupBy
from pandas.core.generic import NDFrame
from pandas._typing import AnyArrayLike
from pipda import register_func

from ..core.utils import ensure_nparray, logger, regcall
from ..core.contexts import Context
from ..core.collections import Collection


@register_func(None, context=Context.EVAL)
def seq_along(along_with: Union[AnyArrayLike, Sequence]) -> np.ndarray:
    """Generate sequences along an iterable

    Args:
        along_with: An iterable to seq along with

    Returns:
        The generated sequence.
    """
    return np.arange(len(along_with)) + 1


@register_func(None, context=Context.EVAL)
def seq_len(length_out: Union[int, AnyArrayLike, Sequence]) -> np.ndarray:
    """Generate sequences with the length"""
    if isinstance(length_out, SeriesGroupBy):
        return length_out.apply(seq_len).explode().astype(int)

    if is_scalar(length_out):
        return np.arange(int(length_out)) + 1

    if len(length_out) > 1:
        logger.warning(
            "In seq_len(...) : first element used of 'length_out' argument"
        )
    length_out = int(list(length_out)[0])
    return np.arange(length_out)


@register_func(None, context=Context.EVAL)
def seq(
    from_: int = None,
    to: int = None,
    by: int = None,
    length_out: Union[int, AnyArrayLike, Sequence[int]] = None,
    along_with: int = None,
) -> AnyArrayLike:
    """Generate a sequence

    https://rdrr.io/r/base/seq.html

    Note that this API is consistent with r-base's seq. 1-based and inclusive.
    """
    if along_with is not None:
        return seq_along(along_with)

    if not is_scalar(from_):
        return seq_along(from_)

    if length_out is not None and from_ is None and to is None:
        return seq_len(length_out)

    if from_ is None:
        from_ = 1
    elif to is None:
        from_, to = 1, from_

    if length_out is not None:
        by = (float(to) - float(from_)) / float(length_out)
    elif by is None:
        by = 1 if to > from_ else -1
        length_out = to - from_ + 1 if to > from_ else from_ - to + 1
    else:
        length_out = (to - from_ + 1.1 * by) // by

    return np.array(
        [from_ + n * by for n in range(int(length_out))]
    )


@register_func(None, context=Context.UNSET)
def c(*elems: Any) -> np.ndarray:
    """Mimic R's concatenation. Named one is not supported yet
    All elements passed in will be flattened.

    Args:
        *elems: The elements

    Returns:
        A collection of elements
    """
    return Collection(*elems)


@register_func(None, context=Context.EVAL)
def rep(
    x: Any,
    times: Union[int, Sequence[int], AnyArrayLike] = 1,
    length: int = None,
    each: int = 1,
) -> np.ndarray:
    """replicates the values in x

    Args:
        x: a vector or scaler
        times: number of times to repeat each element if of length len(x),
            or to repeat the whole vector if of length 1
        length: non-negative integer. The desired length of the output vector
        each: non-negative integer. Each element of x is repeated each times.

    Returns:
        A list of repeated elements in x.
    """
    x = ensure_nparray(x)
    if not is_scalar(times):
        if len(times) != len(x):
            raise ValueError(
                "Invalid times argument, expect length "
                f"{len(times)}, got {len(x)}"
            )
        if each != 1:
            raise ValueError(
                "Unexpected each argument when times is an iterable."
            )

    if is_integer(times) and is_scalar(times):
        x = np.tile(x.repeat(each), times)
    else:
        x = x.repeat(times)
    if length is None:
        return x

    repeats = length // len(x) + 1
    x = np.tile(x, repeats)
    return x[:length]


@singledispatch
def _length(x):
    if is_scalar(x):
        return 1

    return len(x)


@_length.register(np.ndarray)
@_length.register(NDFrame)
def _(x):
    return x.size


@_length.register(GroupBy)
def _(x):
    return x.count()


@register_func(None, context=Context.EVAL)
def length(x):
    return _length(x)


@register_func(None, context=Context.EVAL)
def lengths(x):

    if is_scalar(x):
        return np.array([1], dtype=int)

    if isinstance(x, SeriesGroupBy):
        x = x.obj

    if isinstance(x, Series):
        return x.transform(_length)

    return np.array([_length(elem) for elem in x], dtype=int)


@singledispatch
def _order(x, decreasing: bool = False, na_last: bool = None):

    x = ensure_nparray(x)
    if pd.isnull(na_last):
        x = x[~pd.isnull(x)]
    else:
        if not na_last or decreasing:
            na = -np.inf
        else:
            na = np.inf
        x = np.nan_to_num(x, nan=na)

    out = np.argsort(x)
    return out[::-1] if decreasing else out


@_order.register(Series)
def _(x, decreasing: bool = False, na_last: bool = None):
    if pd.isnull(na_last):
        na_last = True

    if not na_last or decreasing:
        na = -np.inf
    else:
        na = np.inf

    out = x.fillna(na).argsort()
    if decreasing:
        out = out[::-1]

    out.index = x.index
    return out


@_order.register(SeriesGroupBy)
def _(x, decreasing: bool = False, na_last: bool = None):
    return x.apply(_order, decreasing=decreasing, na_last=na_last).droplevel(
        -1
    )


@register_func(None, context=Context.EVAL)
def order(
    x: Iterable,
    decreasing: bool = False,
    na_last: bool = None,
) -> np.ndarray:
    """Sorting or Ordering Vectors

    Args:
        x: A vector to be sorted
        decreasing: Should the vector sort be increasing or decreasing?
        na_last: for controlling the treatment of `NA`s.  If `True`, missing
            values in the data are put last; if `FALSE`, they are put
            first. If None, they are removed.
            For Series and SeriesGroupBy objects, it defaults to True

    Returns:
        The sorted array
    """
    return _order(x, decreasing=decreasing, na_last=na_last)


@register_func(None, context=Context.EVAL)
def rev(x: Iterable) -> Iterable:
    """Get reversed vector"""
    if isinstance(x, SeriesGroupBy):
        _rev = lambda elems: regcall(rev, elems)
        return x.apply(_rev).droplevel(-1)

    if is_scalar(x):
        return x

    return x[::-1]


@register_func(None, context=Context.EVAL)
def unique(x: Iterable[Any]) -> AnyArrayLike:
    """Get unique elements"""
    if is_scalar(x):
        return x

    if isinstance(x, SeriesGroupBy):
        return x.unique().explode().astype(x.obj.dtype)

    return pd.unique(x)  # keeps order


@register_func(None, context=Context.EVAL)
def sample(
    x: Union[int, Iterable],
    size: int = None,
    replace: bool = False,
    prob: Iterable = None,
) -> Iterable[Any]:
    """Takes a sample of the specified size from the elements of x using
    either with or without replacement.

    https://rdrr.io/r/base/sample.html

    Args:
        x: either a vector of one or more elements from which to choose,
            or a positive integer.
        n: a positive number, the number of items to choose from.
        size: a non-negative integer giving the number of items to choose.
        replace: should sampling be with replacement?
        prob: a vector of probability weights for obtaining the elements of
            the vector being sampled.

    Returns:
        A vector of length size with elements drawn from either x or from the
        integers 1:x.
    """
    if isinstance(x, SeriesGroupBy):
        return (
            x.apply(
                lambda elems: np.random.choice(
                    elems,
                    size,
                    replace=replace,
                    p=prob,
                )
            )
            .explode()
            .astype(x.obj.dtype)
        )

    if isinstance(x, str):
        x = list(x)
    if size is None:
        size = len(x) if not is_scalar(x) else x
    return np.random.choice(x, int(size), replace=replace, p=prob)


@register_func(None, context=Context.EVAL)
def sort(
    x: Iterable[Any],
    decreasing: bool = False,
    na_last: bool = None,
) -> AnyArrayLike:
    """Sorting or Ordering Vectors

    Args:
        x: A vector to be sorted
        decreasing: Should the vector sort be increasing or decreasing?
        na_last: for controlling the treatment of `NA`s.  If `True`, missing
            values in the data are put last; if `FALSE`, they are put
            first; if `None`, they are removed.

    Returns:
        The sorted array
    """
    if is_scalar(x):
        return x

    idx = _order(x, decreasing=decreasing, na_last=na_last)
    if isinstance(x, SeriesGroupBy):
        out = x.obj[idx.values]
    elif isinstance(x, Series):
        out = x[idx.values]
    else:
        x = ensure_nparray(x)
        if pd.isnull(na_last):
            x = x[~pd.isnull(x)]

        out = x[idx]

    return out


@singledispatch
def _match(x, table: Iterable, nomatch: Any = -1):
    sorter = np.argsort(table)
    searched = np.searchsorted(table, x, sorter=sorter).ravel()
    out = sorter.take(searched, mode="clip")
    out[~np.isin(x, table)] = nomatch
    return out


@_match.register(Series)
def _(x: Series, table: Iterable, nomatch: Any = -1):
    out = _match(x.values, table, nomatch)
    return Series(out, index=x.index, name=x.name)


@_match.register(SeriesGroupBy)
def _(x: SeriesGroupBy, table: Iterable, nomatch: Any = -1):
    return x.apply(_match, table=table, nomatch=nomatch).explode().astype(int)


@register_func(None, context=Context.EVAL)
def match(
    x: Any,
    table: Iterable,
    nomatch: Any = -1,
    # incomparables ...,
) -> Iterable[int]:
    """match returns a vector of the positions of (first) matches of
    its first argument in its second.

    See stackoverflow#4110059/pythonor-numpy-equivalent-of-match-in-r

    Args:
        x: The values to be matched
        table: The values to be matched against
        nomatch: The value to be returned in the case when no match is found
            Instead of NA in R, this function takes -1 for non-matched elements
            to keep the type as int.
    """
    return _match(x, table, nomatch)
