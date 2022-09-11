"""Subset rows using their positions

https://github.com/tidyverse/dplyr/blob/master/R/slice.R
"""
import builtins
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Union

import numpy as np
from pipda import register_verb, Expression

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.api.types import is_integer
from ..core.backends.pandas.core.groupby import SeriesGroupBy

from ..core.collections import Collection
from ..core.broadcast import _ungroup
from ..core.contexts import Context
from ..core.utils import dict_get, logger
from ..core.tibble import Tibble, TibbleGrouped, TibbleRowwise

if TYPE_CHECKING:
    from ..core.backends.pandas import Index


@register_verb(DataFrame, context=Context.SELECT)
def slice(
    _data: DataFrame,
    *rows: Union[int, str],
    _preserve: bool = False,
) -> Tibble:
    """Index rows by their (integer) locations

    Original APIs https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: The dataframe
        rows: The indexes
            Ranges can be specified as `c[1:3]`
            Note that the negatives mean differently than in dplyr.
            In dplyr, negative numbers meaning exclusive, but here negative
            numbers are negative indexes like how they act in python indexing.
            For exclusive indexes, you need to use inversion. For example:
            `slice(df, ~c[:3])` excludes first 3 rows. You can also do:
            `slice(df, ~c(c[:3], 6))` to exclude multiple set of rows.
            To exclude a single row, you can't do this directly: `slice(df, ~1)`
            since `~1` is directly compiled into a number. You can do this
            instead: `slice(df, ~c(1))`
            Exclusive and inclusive expressions are allowed to be mixed, unlike
            in `dplyr`. They are expanded in the order they are passed in.
        _preserve: Just for compatibility with `dplyr`'s `filter`.
            It's always `False` here.

    Returns:
        The sliced dataframe
    """
    # if _preserve:
    #     logger.warning("`slice()` doesn't support `_preserve` argument yet.")

    if not rows:
        return _data.copy()

    rows = _sanitize_rows(rows, _data.shape[0])
    return _data.take(rows)


@slice.register(TibbleGrouped, context=Context.SELECT)
def _(
    _data: TibbleGrouped,
    *rows: Any,
    _preserve: bool = False,
) -> TibbleGrouped:
    """Slice on grouped dataframe"""
    if _preserve:
        logger.warning("`slice()` doesn't support `_preserve` argument yet.")

    grouped = _data._datar["grouped"]
    indices = _sanitize_rows(
        rows,
        grouped.grouper.indices,
        grouped.grouper.result_index,
    )

    return _data.take(indices)


@register_verb(DataFrame, context=Context.EVAL)
def slice_head(
    _data: DataFrame,
    n: int = None,
    prop: float = None,
) -> Tibble:
    """Select first rows

    Args:
        _data: The dataframe.
        n: and
        prop: Provide either n, the number of rows, or prop, the proportion of
            rows to select.
            If neither are supplied, n = 1 will be used.
            If n is greater than the number of rows in the group (or prop > 1),
            the result will be silently truncated to the group size.
            If the proportion of a group size is not an integer,
            it is rounded down.

    Returns:
        The sliced dataframe
    """
    n = _n_from_prop(_data.shape[0], n, prop)
    return slice(
        _data,
        builtins.slice(None, n),
        __ast_fallback="normal",
    )


@slice_head.register(TibbleGrouped, context=Context.EVAL)
def _(
    _data: DataFrame,
    n: int = None,
    prop: float = None,
) -> TibbleGrouped:
    """Slice on grouped dataframe"""
    grouped = _data._datar["grouped"]
    # Calculate n's of each group
    ns = grouped.grouper.size().transform(lambda x: _n_from_prop(x, n, prop))
    # Get indices of each group
    # A better way?
    indices = np.concatenate(
        [
            grouped.grouper.indices[key][: ns[key]]
            for key in grouped.grouper.result_index
        ]
    )

    return _data.take(indices)


@slice_head.register(TibbleRowwise, context=Context.EVAL)
def _(
    _data: TibbleRowwise,
    n: int = None,
    prop: float = None,
) -> TibbleRowwise:
    """Slice on grouped dataframe"""
    n = _n_from_prop(1, n, prop)

    if n >= 1:
        return _data.copy()

    return _data.take([])


@register_verb(DataFrame, context=Context.EVAL)
def slice_tail(
    _data: DataFrame,
    n: int = 1,
    prop: float = None,
) -> Tibble:
    """Select last rows

    See Also:
        [`slice_head()`](datar.dplyr.slice.slice_head)
    """
    n = _n_from_prop(_data.shape[0], n, prop)
    return slice(
        _data,
        builtins.slice(-n, None),
        __ast_fallback="normal",
    )


@slice_tail.register(TibbleGrouped, context=Context.EVAL)
def _(
    _data: DataFrame,
    n: int = None,
    prop: float = None,
) -> TibbleGrouped:
    """Slice on grouped dataframe"""
    grouped = _data._datar["grouped"]
    # Calculate n's of each group
    ns = grouped.grouper.size().transform(lambda x: _n_from_prop(x, n, prop))
    # Get indices of each group
    # A better way?
    indices = np.concatenate(
        [
            grouped.grouper.indices[key][-ns[key] :]
            for key in grouped.grouper.result_index
        ]
    )

    return _data.take(indices)


@slice_tail.register(TibbleRowwise, context=Context.PENDING)
def _(
    _data: TibbleRowwise,
    n: int = None,
    prop: float = None,
) -> TibbleRowwise:
    """Slice on grouped dataframe"""
    return slice_head(
        _data,
        n=n,
        prop=prop,
        __ast_fallback="normal",
    )


@register_verb(DataFrame, context=Context.EVAL)
def slice_min(
    _data: DataFrame,
    order_by: Expression,
    n: int = 1,
    prop: float = None,
    with_ties: Union[bool, str] = True,
) -> Tibble:
    """select rows with lowest values of a variable.

    Args:
        order_by: Variable or function of variables to order by.
        n: and
        prop: Provide either n, the number of rows, or prop, the proportion of
            rows to select.
            If neither are supplied, n = 1 will be used.
            If n is greater than the number of rows in the group (or prop > 1),
            the result will be silently truncated to the group size.
            If the proportion of a group size is not an integer,
            it is rounded down.
        with_ties: Should ties be kept together?
            The default, `True`, may return more rows than you request.
            Use `False` to ignore ties, and return the first n rows.
    """
    if isinstance(_data, TibbleGrouped) and prop is not None:
        raise ValueError(
            "`slice_min()` doesn't support `prop` for grouped data yet."
        )

    if isinstance(_data, TibbleRowwise):
        n = _n_from_prop(1, n, prop)
    else:
        n = _n_from_prop(_data.shape[0], n, prop)

    sliced = order_by.nsmallest(n, keep="all" if with_ties else "first")
    sliced = sliced[~pd.isnull(sliced)]
    return _data.reindex(sliced.index.get_level_values(-1))


@register_verb(DataFrame, context=Context.EVAL)
def slice_max(
    _data: DataFrame,
    order_by: Iterable[Any],
    n: int = 1,
    prop: float = None,
    with_ties: Union[bool, str] = True,
) -> DataFrame:
    """select rows with highest values of a variable.

    See Also:
        [`slice_head()`](datar.dplyr.slice.slice_head)
    """
    if isinstance(_data, TibbleGrouped) and prop is not None:
        raise ValueError(
            "`slice_max()` doesn't support `prop` for grouped data yet."
        )

    if isinstance(_data, TibbleRowwise):
        n = _n_from_prop(1, n, prop)
    else:
        n = _n_from_prop(_data.shape[0], n, prop)

    sliced = order_by.nlargest(n, keep="all" if with_ties else "first")
    sliced = sliced[~pd.isnull(sliced)]
    return _data.reindex(sliced.index.get_level_values(-1))


@register_verb(DataFrame, context=Context.EVAL)
def slice_sample(
    _data: DataFrame,
    n: int = 1,
    prop: float = None,
    weight_by: Iterable[Union[int, float]] = None,
    replace: bool = False,
    random_state: Any = None,
) -> DataFrame:
    """Randomly selects rows.

    See Also:
        [`slice_head()`](datar.dplyr.slice.slice_head)
    """
    if (
        prop is not None
        and isinstance(_data, TibbleGrouped)
        and not isinstance(_data, TibbleRowwise)
    ):
        raise ValueError(
            "`slice_sample()` doesn't support `prop` for grouped data yet."
        )

    if isinstance(_data, TibbleRowwise):
        n = _n_from_prop(1, n, prop)
    else:
        n = _n_from_prop(_data.shape[0], n, prop)

    if n == 0:
        # otherwise _data.sample raises error when weight_by is empty as well
        return _data.take([])

    return _data.sample(
        n=n,
        replace=replace,
        weights=_ungroup(weight_by),
        random_state=random_state,
    )


def _n_from_prop(
    total: int,
    n: int = None,
    prop: float = None,
) -> int:
    """Get n from a proportion"""
    if n is None and prop is None:
        return 1
    if n is not None and not isinstance(n, int):
        raise TypeError(f"Expect `n` int type, got {type(n)}.")
    if prop is not None and not isinstance(prop, (int, float)):
        raise TypeError(f"Expect `prop` a number, got {type(n)}.")
    if (n is not None and n < 0) or (prop is not None and prop < 0):
        raise ValueError("`n` and `prop` should not be negative.")
    if prop is not None:
        return int(float(total) * min(prop, 1.0))
    return min(n, total)


def _sanitize_rows(
    rows: Iterable,
    indices: Union[int, Mapping] = None,
    result_index: "Index" = None,
) -> np.ndarray:
    """Sanitize rows passed to slice"""
    from ..base import c

    if is_integer(indices):
        rows = Collection(*rows, pool=indices)
        if rows.error:
            raise rows.error from None
        return np.array(rows, dtype=int)

    out = []
    if any(isinstance(row, SeriesGroupBy) for row in rows):
        rows = c(*rows)
        for key in result_index:
            idx = dict_get(indices, key)
            if idx.size == 0:
                continue

            gidx = dict_get(rows.grouper.indices, key)
            out.extend(idx.take(rows.obj.take(gidx)))
    else:
        for key in result_index:
            idx = dict_get(indices, key)
            if idx.size == 0:
                continue
            grows = Collection(*rows, pool=idx.size)
            if grows.error:
                raise grows.error from None
            out.extend(idx.take(grows))

    return np.array(out, dtype=int)
