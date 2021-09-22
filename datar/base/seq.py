"""Generating and manipulating sequences"""

from typing import Any, Iterable, Union

import numpy
import pandas
from pipda import register_func

from ..core.collections import Collection
from ..core.contexts import Context
from ..core.types import (
    ArrayLikeType,
    IntOrIter,
    IntType,
    NumericType,
    is_iterable,
    is_not_null,
    is_scalar,
    is_scalar_int,
    is_null,
)
from ..core.utils import Array, get_option, length_of, logger


@register_func(None, context=Context.EVAL)
def seq_along(along_with: Iterable[Any], base0_: bool = None) -> ArrayLikeType:
    """Generate sequences along an iterable

    Args:
        along_with: An iterable to seq along with
        base0_: Whether the generated sequence should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The generated sequence.
    """
    base0_ = get_option("index.base.0", base0_)
    return Array(range(len(along_with))) + int(not base0_)


@register_func(None, context=Context.EVAL)
def seq_len(length_out: IntOrIter, base0_: bool = None) -> ArrayLikeType:
    """Generate sequences with the length"""
    base0_ = get_option("index.base.0", base0_)
    if is_scalar(length_out):
        return Array(range(int(length_out))) + int(not base0_)
    if len(length_out) > 1:
        logger.warning(
            "In seq_len(%r) : first element used of 'length_out' argument",
            length_out,
        )
    length_out = int(list(length_out)[0])
    return Array(range(length_out)) + int(not base0_)


@register_func(None, context=Context.EVAL)
def seq(
    from_: IntType = None,
    to: IntType = None,
    by: IntType = None,
    length_out: IntType = None,
    along_with: IntType = None,
    base0_: bool = None,
) -> ArrayLikeType:
    """Generate a sequence

    https://rdrr.io/r/base/seq.html

    Note that this API is consistent with r-base's seq. 1-based and inclusive.
    """
    base0_ = get_option("index.base.0", base0_)
    if along_with is not None:
        return seq_along(along_with, base0_)
    if from_ is not None and not is_scalar(from_):
        return seq_along(from_, base0_)
    if length_out is not None and from_ is None and to is None:
        return seq_len(length_out)

    base = int(not base0_)

    if from_ is None:
        from_ = base
    elif to is None:
        from_, to = base, from_

    if length_out is not None:
        by = (float(to) - float(from_)) / float(length_out - 1)

    elif by is None:
        by = 1 if to > from_ else -1
        length_out = to - from_ + base if to > from_ else from_ - to + base
    else:
        length_out = (to - from_ + 0.1 * by + float(base) * by) // by
    return Array([from_ + n * by for n in range(int(length_out))])


@register_func(None, context=Context.EVAL)
def rep(
    x: Any,
    times: IntOrIter = 1,
    length: int = None,
    each: int = 1,
) -> ArrayLikeType:
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
    if is_scalar(x):
        x = [x]
    if is_iterable(times):
        if len(times) != len(x):
            raise ValueError(
                "Invalid times argument, expect length "
                f"{len(times)}, got {len(x)}"
            )
        if each != 1:
            raise ValueError(
                "Unexpected each argument when times is an iterable."
            )

    if is_scalar_int(times):
        x = Array([elem for elem in x for _ in range(each)] * int(times))
    else:
        x = Array([elem for n, elem in zip(times, x) for _ in range(n)])
    if length is None:
        return x
    repeats = length // len(x) + 1
    x = numpy.tile(x, repeats)
    return x[:length]


@register_func(None, context=Context.EVAL)
def rev(x: Iterable[Any]) -> numpy.ndarray:
    """Get reversed vector"""
    dtype = getattr(x, "dtype", None)
    return Array(list(reversed(x)), dtype=dtype)


@register_func(None, context=Context.EVAL)
def unique(x: Iterable[Any]) -> numpy.ndarray:
    """Get unique elements"""
    # return numpy.unique(x)
    return pandas.unique(x)  # keeps order


@register_func(None, context=Context.EVAL)
def sort(
    x: Iterable[Any],
    decreasing: bool = False,
    na_last: bool = None,
) -> numpy.ndarray:
    """Sorting or Ordering Vectors

    Args:
        x: A vector to be sorted
        decreasing: Should the vector sort be increasing or decreasing?
        na_last: for controlling the treatment of `NA`s.  If `True`, missing
            values in the data are put last; if `FALSE`, they are put
            first; if `NA`, they are removed.

    Returns:
        The sorted array
    """
    x = Array(x)
    not_na_idx = is_not_null(x)
    non_nas = x[not_na_idx]
    out = numpy.sort(non_nas)
    if decreasing:
        out = out[::-1]
    if na_last is None:
        return out

    nas = x[~not_na_idx].tolist()
    if na_last:
        return Array(out.tolist() + nas)
    return Array(nas + out.tolist())


@register_func(None, context=Context.EVAL)
def order(
    x: Iterable[Any],
    na_last: bool = None,
    decreasing: bool = False,
    # method?
    base0_: bool = None,
) -> numpy.ndarray:
    """Returns a permutation which rearranges its first argument
    into ascending or descending order

    Args:
        x: A vector to order
        decreasing: Should the vector sort be increasing or decreasing?
        na_last: for controlling the treatment of `NA`s.  If `True`, missing
            values in the data are put last; if `FALSE`, they are put
            first; if `NA`, they are removed.
        base0_: Return a 0-based indices or not.
            If not given, will use `get_option('which.base.0')`

    Returns:
        The ordered indices of the vector
    """
    na_mask = is_null(x)
    na_idx = numpy.flatnonzero(na_mask).tolist()
    out_idx = numpy.argsort(x)
    if decreasing:
        out_idx = out_idx[::-1]

    non_na_idx = [idx for idx in out_idx if idx not in na_idx]
    if na_last is None:
        out = non_na_idx
    elif na_last:
        out = non_na_idx + na_idx
    else:
        out = na_idx + non_na_idx

    out = Array(out, dtype=int)
    return out if base0_ else out + 1


length = register_func(None, context=Context.EVAL, func=length_of)


@register_func(None, context=Context.EVAL)
def lengths(x: Any) -> IntOrIter:
    """Lengths of elements in x"""
    if is_scalar(x):
        return Array([1], dtype=numpy.int_)
    return Array([length(elem) for elem in x], dtype=numpy.int_)


@register_func(None, context=Context.EVAL)
def sample(
    x: Union[IntType, Iterable[Any]],
    size: int = None,
    replace: bool = False,
    prob: Iterable[NumericType] = None,
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
    if isinstance(x, str):
        x = list(x)
    if size is None:
        size = len(x) if is_iterable(x) else x
    return numpy.random.choice(x, int(size), replace=replace, p=prob)


@register_func(None, context=Context.UNSET)
def c(*elems: Any) -> Collection:
    """Mimic R's concatenation. Named one is not supported yet
    All elements passed in will be flattened.

    Args:
        *elems: The elements

    Returns:
        A collection of elements
    """
    return Collection(*elems)


@register_func(None, context=Context.EVAL)
def match(
    x: Any,
    table: Iterable,
    nomatch: Any = -1,
    # incomparables ...,
    base0_: bool = None,
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
        base0_: Whether return indices are 0-based or not.
            If not provided, will use `get_option('which_base_0')`
    """
    base = int(not get_option("which_base_0", base0_))
    return Array(
        [
            list(table).index(elem) + base if elem in table else nomatch
            for elem in x
        ],
        dtype=int,
    )
