import numpy as np
import pandas as pd
from pandas.api.types import is_scalar, is_integer
from pandas.core.generic import NDFrame
from pandas.core.groupby import SeriesGroupBy, GroupBy
from pipda import register_func

from ..core.utils import ensure_nparray, logger, regcall
from ..core.factory import func_factory
from ..core.contexts import Context
from ..core.collections import Collection
from ..core.tibble import TibbleGrouped, reconstruct_tibble


@register_func(None, context=Context.EVAL)
def seq_along(along_with):
    """Generate sequences along an iterable

    Args:
        along_with: An iterable to seq along with

    Returns:
        The generated sequence.
    """
    return np.arange(len(along_with)) + 1


@register_func(None, context=Context.EVAL)
def seq_len(length_out):
    """Generate sequences with the length"""
    if isinstance(length_out, SeriesGroupBy):
        return length_out.apply(seq_len.__origfunc__).explode().astype(int)

    if is_scalar(length_out):
        return np.arange(int(length_out)) + 1

    if len(length_out) > 1:
        logger.warning(
            "In seq_len(...) : first element used of 'length_out' argument"
        )
    length_out = int(list(length_out)[0])
    return np.arange(length_out) + 1


@register_func(None, context=Context.EVAL)
def seq(
    from_=None,
    to=None,
    by=None,
    length_out=None,
    along_with=None,
):
    """Generate a sequence

    https://rdrr.io/r/base/seq.html

    Note that this API is consistent with r-base's seq. 1-based and inclusive.
    """
    if along_with is not None:
        return regcall(seq_along, along_with)

    if not is_scalar(from_):
        return regcall(seq_along, from_)

    if length_out is not None and from_ is None and to is None:
        return regcall(seq_len, length_out)

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

    return np.array([from_ + n * by for n in range(int(length_out))])


@register_func(None, context=Context.UNSET)
def c(*elems):
    """Mimic R's concatenation. Named one is not supported yet
    All elements passed in will be flattened.

    Args:
        *elems: The elements

    Returns:
        A collection of elements
    """
    return Collection(*elems)


@func_factory("apply")
def rep(
    x,
    times=1,
    length=None,
    each=1
):
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


rep.register(
    SeriesGroupBy,
    func=None,
    post=lambda out, x, *args, **kwargs: out.explode().astype(x.obj.dtype)
)


rep.register(
    TibbleGrouped,
    func=None,
    post=lambda out, x, *args, **kwargs: reconstruct_tibble(x, out)
)


@func_factory("agg")
def length(x):
    """Get length of elements"""
    if is_scalar(x):
        return 1

    return len(x)


length.register(GroupBy, "count")


@func_factory("transform")
def lengths(x):
    """Get Lengths of elementss of a vector"""
    # optimize according to dtype?
    if is_scalar(x):
        return np.array([1], dtype=int)

    return np.array([length.__raw__(elem) for elem in x], dtype=int)


@func_factory("apply")
def order(x, decreasing=False, na_last=None):
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


order.register(
    SeriesGroupBy,
    func=None,
    post=(
        lambda out, x, decreasing=False, na_last=None:
        out.explode().astype(int)
    ),
)


@func_factory("apply")
def rev(x):
    """Get reversed vector"""
    if is_scalar(x):
        return x

    return x[::-1]


rev.register(
    NDFrame,
    func=None,
    post=lambda out, x: setattr(out, "index", x.index) or out
)


rev.register(
    GroupBy,
    func=None,
    post=lambda out, x: out.droplevel(-1)
)


# @register_func(None, context=Context.EVAL)
# def unique(x):
#     """Get unique elements"""
#     if is_scalar(x):
#         return x

#     if isinstance(x, SeriesGroupBy):
#         return x.unique().explode().astype(x.obj.dtype)

#     return pd.unique(x)  # keeps order


@func_factory("apply")
def sample(
    x,
    size=None,
    replace=False,
    prob=None,
):
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
        size = len(x) if not is_scalar(x) else x
    return np.random.choice(x, int(size), replace=replace, p=prob)


@func_factory("apply")
def sort(
    x,
    decreasing=False,
    na_last=None,
):
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

    idx = order.__raw__(x, decreasing=decreasing, na_last=na_last)
    x = ensure_nparray(x)
    if pd.isnull(na_last):
        x = x[~pd.isnull(x)]

    return x[idx]


@func_factory("apply")
def match(x, table, nomatch=-1):
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
    x = ensure_nparray(x)
    table = ensure_nparray(table)
    sorter = np.argsort(table)
    searched = np.searchsorted(table, x, sorter=sorter).ravel()
    out = sorter.take(searched, mode="clip")
    out[~np.isin(x, table)] = nomatch
    return out


@match.register(SeriesGroupBy, replace=True)
def _(x, table, nomatch=-1):
    if not isinstance(table, SeriesGroupBy):
        return (
            x.apply(match.__raw__, table=table, nomatch=nomatch)
            .explode()
            .astype(int)
        )

    from ..tibble import tibble

    df = tibble(x=x, table=table)  # TibbleGrouped
    return (
        df._datar["grouped"]
        .apply(lambda g: match.__raw__(g["x"], g["table"], nomatch=nomatch))
        .explode()
        .astype(int)
    )
