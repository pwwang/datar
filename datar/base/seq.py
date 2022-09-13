import numpy as np
from pipda import register_func

from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar
from ..core.backends.pandas.core.groupby import SeriesGroupBy, GroupBy

from ..core.utils import ensure_nparray, logger
from ..core.broadcast import _grouper_compatible
from ..core.factory import func_factory
from ..core.contexts import Context
from ..core.collections import Collection, register_subscr_func
from ..core.tibble import TibbleGrouped


@register_func(context=Context.EVAL)
def seq_along(along_with):
    """Generate sequences along an iterable

    Args:
        along_with: An iterable to seq along with

    Returns:
        The generated sequence.
    """
    return np.arange(len(along_with)) + 1


@register_func(context=Context.EVAL)
def seq_len(length_out):
    """Generate sequences with the length"""
    if isinstance(length_out, SeriesGroupBy):
        return length_out.apply(seq_len.func).explode().astype(int)

    if is_scalar(length_out):
        return np.arange(int(length_out)) + 1

    if len(length_out) > 1:
        logger.warning(
            "In seq_len(...) : first element used of 'length_out' argument"
        )
    length_out = int(list(length_out)[0])
    return np.arange(length_out) + 1


@register_func(context=Context.EVAL)
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

    return np.array([from_ + n * by for n in range(int(length_out))])


@func_factory(kind="agg")
def length(x):
    """Get length of elements"""
    return x.size


length.register((TibbleGrouped, GroupBy), "count")


@func_factory(kind="agg")
def lengths(x):
    """Get Lengths of elementss of a vector"""
    return x.transform(lambda y: 1 if is_scalar(y) else len(y))


@func_factory(kind="transform")
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
        out.explode().astype(int).groupby(
            x.grouper,
            observed=x.observed,
            sort=x.sort,
            dropna=x.dropna,
        )
    ),
)


@func_factory(kind="transform")
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


@func_factory()
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


@func_factory(kind="transform")
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
    idx = order.func(x, decreasing=decreasing, na_last=na_last).values
    out = x.iloc[idx]
    out.index = x.index
    return out


@register_func(context=Context.EVAL)
def match(x, table, nomatch=-1):
    """match returns a vector of the positions of (first) matches of
    its first argument in its second.

    See stackoverflow#4110059/pythonor-numpy-equivalent-of-match-in-r

    Note that the returned vector is 0-based.
    When `x` is grouped and table is a series from aggregated-like results, for
    example, `unique(x)` then each group will match separately.

    Examples:
        >>> match([1, 2, 3], [2, 3, 4])
        >>> # [-1, 0, 1]
        >>> df = tibble(x=[1, 1, 2, 2], y=["a", "b", "b", "b"])
        >>> match(df.y, unique(df.y))
        >>> # [0, 1, 1, 1]
        >>> gf = df >> group_by(f.x)
        >>> match(gf.y, unique(gf.y))
        >>> # [0, 1, 0, 0] (grouped)

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

    if isinstance(x, SeriesGroupBy):
        # length of each group may differ
        # table could be, for example, unique elements of each group in x
        x1 = x.agg(tuple)
        x1 = x1.groupby(
            x1.index,
            observed=x.observed,
            sort=x.sort,
            dropna=x.dropna,
        )
        df = x1.obj.to_frame()
        if isinstance(table, SeriesGroupBy):
            t1 = table.agg(tuple)
            t1 = t1.groupby(
                t1.index,
                observed=table.observed,
                sort=table.sort,
                dropna=table.dropna,
            )
            if not _grouper_compatible(x1.grouper, t1.grouper):
                raise ValueError("Grouping of x and table are not compatible")
            df["table"] = t1.obj
        elif isinstance(table, Series):
            t1 = table.groupby(
                table.index,
                observed=True,
                sort=False,
                dropna=False,
            ).agg(tuple)
            t1 = t1.groupby(
                t1.index,
                observed=x1.observed,
                sort=x1.sort,
                dropna=x1.dropna,
            )
            if not _grouper_compatible(x1.grouper, t1.grouper):
                df["table"] = [ensure_nparray(table)] * df.shape[0]
            else:
                df["table"] = t1.obj
        else:
            df["table"] = [ensure_nparray(table)] * df.shape[0]

        out = (
            df
            # not working for pandas 1.3.0
            # .agg(lambda row: match_dummy(*row), axis=1)
            .apply(lambda row: match_dummy(*row), axis=1)
            .explode()
            .astype(int)
        ).groupby(
            x.grouper,
            observed=x.observed,
            sort=x.sort,
            dropna=x.dropna,
        )
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

    if isinstance(x, Series):
        return Series(match_dummy(x, table), index=x.index)

    return match_dummy(x, table)


@register_subscr_func
def c(*elems):
    """Mimic R's concatenation. Named one is not supported yet
    All elements passed in will be flattened.

    Args:
        *elems: The elements

    Returns:
        A collection of elements
    """
    if not any(isinstance(elem, SeriesGroupBy) for elem in elems):
        return Collection(*elems)

    from ..tibble import tibble

    values = []
    for elem in elems:
        if isinstance(elem, SeriesGroupBy):
            values.append(elem.agg(list))
        elif is_scalar(elem):
            values.append(elem)
        else:
            values.extend(elem)

    df = tibble(*values)
    # pandas 1.3.0 expand list into columns after aggregation
    # pandas 1.3.2 has this fixed
    # https://github.com/pandas-dev/pandas/issues/42727
    out = df.agg(
        lambda row: Collection(*row),
        axis=1,
    )
    if isinstance(out, DataFrame):  # pragma: no cover
        # pandas < 1.3.2
        out = Series(out.values.tolist(), index=out.index, dtype=object)

    out = out.explode().convert_dtypes()
    # TODO: check observed, sort and dropna?
    out = out.reset_index(drop=True).groupby(out.index)
    return out
