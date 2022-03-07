import numpy as np
from pandas import Series
from pandas.api.types import is_scalar, is_integer
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


@func_factory("apply", "x")
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


@func_factory("agg", "x")
def length(x):
    """Get length of elements"""
    return x.size


length.register((TibbleGrouped, GroupBy), "count")


@func_factory("agg", "x")
def lengths(x):
    """Get Lengths of elementss of a vector"""
    return x.transform(lambda y: 1 if is_scalar(y) else len(y))


@func_factory("transform", "x")
def order(x: Series, decreasing=False, na_last=True):
    """Sorting or Ordering Vectors

    Args:
        x: A vector to be sorted
        decreasing: Should the vector sort be increasing or decreasing?
        na_last: for controlling the treatment of `NA`s.  If `True`, missing
            values in the data are put last; if `FALSE`, they are put
            first.

    Returns:
        The sorted array
    """
    if not na_last or decreasing:
        na = -np.inf
    else:
        na = np.inf

    out = np.argsort(x.fillna(na))
    if decreasing:
        out = out[::-1]
        out.index = x.index
    return out


order.register(
    SeriesGroupBy,
    func=None,
    post=(
        lambda out, x, decreasing=False, na_last=None:
        out.explode().astype(int).groupby(x.grouper)
    ),
)


@func_factory("transform", "x")
def rev(x, __args_raw=None):
    """Get reversed vector"""
    rawx = __args_raw["x"]
    if isinstance(rawx, (Series, SeriesGroupBy)):  # groupby from transform()
        out = x[::-1]
        out.index = x.index
        return out

    if is_scalar(rawx):
        return np.array([rawx], dtype=type(rawx))

    return rawx[::-1]


@func_factory("agg", "x")
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
    if size is None:
        size = len(x) if not is_scalar(x) else x
    elif not is_scalar(size):
        if len(size) > 1:
            raise ValueError(
                "In sample(...): multiple `size`s are not supported yet."
            )
    return np.random.choice(x, int(size), replace=replace, p=prob)


@func_factory("transform", "x")
def sort(
    x,
    decreasing=False,
    na_last=True,
):
    """Sorting or Ordering Vectors

    Args:
        x: A vector to be sorted
        decreasing: Should the vector sort be increasing or decreasing?
        na_last: for controlling the treatment of `NA`s.  If `True`, missing
            values in the data are put last; if `FALSE`, they are put
            first;

    Returns:
        The sorted array
    """
    idx = order.__raw__(x, decreasing=decreasing, na_last=na_last).values
    out = x.iloc[idx]
    out.index = x.index
    return out


@register_func(None, context=Context.EVAL)
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
    def match_dummy(xx, tab):
        sorter = np.argsort(tab)
        if isinstance(sorter, Series):
            sorter = sorter.values
        searched = np.searchsorted(tab, xx, sorter=sorter).ravel()
        out = sorter.take(searched, mode="clip")
        out[~np.isin(xx, tab)] = nomatch
        return out

    if isinstance(x, SeriesGroupBy) and isinstance(table, SeriesGroupBy):
        from ..tibble import tibble

        df = tibble(x=x, y=table)
        return df._datar["grouped"].apply(
            lambda g: match_dummy(g.x, g.y)
        ).explode().astype(int).groupby(x.grouper)

    if isinstance(x, SeriesGroupBy):
        out = x.transform(match_dummy, tab=table).groupby(x.grouper)
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

    # # really needed?
    # if isinstance(table, SeriesGroupBy):
    #     return table.apply(lambda e: match_dummy(x, e)).explode().astype(int)

    if isinstance(x, Series):
        return Series(match_dummy(x, table), index=x.index)

    return match_dummy(x, table)
