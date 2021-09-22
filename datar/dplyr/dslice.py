"""Subset rows using their positions

https://github.com/tidyverse/dplyr/blob/master/R/slice.R
"""
import builtins
from typing import Any, Iterable, List, Union

from pandas import DataFrame, RangeIndex
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.collections import Collection
from ..core.utils import reconstruct_tibble
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.types import NumericOrIter

from ..base import NA, unique
from .group_by import ungroup
from .dfilter import _filter_groups


@register_verb(DataFrame, context=Context.SELECT)
def slice(
    _data: DataFrame,
    *rows: NumericOrIter,
    _preserve: bool = False,
    base0_: bool = None,
) -> DataFrame:
    """Index rows by their (integer) locations

    Original APIs https://dplyr.tidyverse.org/reference/slice.html

    Args:
        _data: The dataframe
        rows: The indexes
            Ranges can be specified as `f[1:3]`
            Note that the negatives mean differently than in dplyr.
            In dplyr, negative numbers meaning exclusive, but here negative
            numbers are negative indexes like how they act in python indexing.
            For exclusive indexes, you need to use inversion. For example:
            `slice(df, ~f[:3])` excludes first 3 rows. You can also do:
            `slice(df, ~c(f[:3], 6))` to exclude multiple set of rows.
            To exclude a single row, you can't do this directly: `slice(df, ~1)`
            since `~1` is directly compiled into a number. You can do this
            instead: `slice(df, ~c(1))`
            Exclusive and inclusive expressions are allowed to be mixed, unlike
            in `dplyr`. They are expanded in the order they are passed in.
        _preserve: Relevant when the _data input is grouped.
            If _preserve = FALSE (the default), the grouping structure is
            recalculated based on the resulting data,
            otherwise the grouping is kept as is.
        base0_: If rows are selected by indexes, whether they are 0-based.
            If not provided, `datar.base.get_option('index.base.0')` is used.

    Returns:
        The sliced dataframe
    """
    if not rows:
        return _data

    rows = _sanitize_rows(rows, _data.shape[0], base0_)
    out = _data.iloc[rows, :]
    if isinstance(_data.index, RangeIndex):
        out.reset_index(drop=True, inplace=True)
    # copy_attrs(out, _data) # attrs carried
    return out


@slice.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy,
    *rows: Any,
    _preserve: bool = False,
    base0_: bool = None,
) -> DataFrameGroupBy:
    """Slice on grouped dataframe"""
    out = _data._datar_apply(
        slice, *rows, base0_=base0_, _drop_index=False
    ).sort_index()
    out = reconstruct_tibble(_data, out)
    gdata = _filter_groups(out, _data)

    if not _preserve and _data.attrs.get("_group_drop", True):
        out.attrs["_group_data"] = gdata[gdata["_rows"].map(len) > 0]

    return out


@slice.register(DataFrameRowwise, context=Context.PENDING)
def _(
    _data: DataFrameRowwise,
    *rows: Any,
    _preserve: bool = False,
    base0_: bool = None,
) -> DataFrameRowwise:
    """Slice on grouped dataframe"""
    out = slice.dispatch(DataFrame)(
        _data,
        *rows,
        _preserve=_preserve,
        base0_=base0_,
    )
    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame)
def slice_head(
    _data: DataFrame, n: int = None, prop: float = None
) -> DataFrame:
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
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@slice_head.register(DataFrameGroupBy, context=Context.PENDING)
def _(_data: DataFrame, n: int = None, prop: float = None) -> DataFrameGroupBy:
    """Slice on grouped dataframe"""
    out = _data._datar_apply(
        slice_head,
        n=n,
        prop=prop,
        _drop_index=False,
        __calling_env=CallingEnvs.REGULAR,
    ).sort_index()
    return reconstruct_tibble(_data, out)


@slice_head.register(DataFrameRowwise, context=Context.PENDING)
def _(
    _data: DataFrameRowwise, n: int = None, prop: float = None
) -> DataFrameRowwise:
    """Slice on grouped dataframe"""
    out = slice_head.dispatch(DataFrame)(_data, n=n, prop=prop)
    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame)
def slice_tail(_data: DataFrame, n: int = 1, prop: float = None) -> DataFrame:
    """Select last rows

    See Also:
        [`slice_head()`](datar.dplyr.slice.slice_head)
    """
    n = _n_from_prop(_data.shape[0], n, prop)
    return slice(
        _data,
        builtins.slice(-n, None),
        base0_=True,
        __calling_env=CallingEnvs.REGULAR,
    )


@slice_tail.register(DataFrameGroupBy, context=Context.PENDING)
def _(_data: DataFrame, n: int = None, prop: float = None) -> DataFrameGroupBy:
    """Slice on grouped dataframe"""
    out = _data._datar_apply(
        slice_tail, n=n, prop=prop, _drop_index=False
    ).sort_index()
    return reconstruct_tibble(_data, out)


@slice_tail.register(DataFrameRowwise, context=Context.PENDING)
def _(
    _data: DataFrameRowwise, n: int = None, prop: float = None
) -> DataFrameRowwise:
    """Slice on grouped dataframe"""
    out = slice_tail.dispatch(DataFrame)(_data, n=n, prop=prop)
    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame, extra_contexts={"order_by": Context.EVAL})
def slice_min(
    _data: DataFrame,
    order_by: Iterable[Any],
    n: int = 1,
    prop: float = None,
    with_ties: Union[bool, str] = True,
) -> DataFrame:
    """select rows with lowest values of a variable.

    See Also:
        [`slice_head()`](datar.dplyr.slice.slice_head)
    """
    n = _n_from_prop(_data.shape[0], n, prop)

    sorting_df = DataFrame(index=_data.index)
    sorting_df["x"] = order_by
    keep = {True: "all", False: "first"}.get(with_ties, with_ties)
    sorting_df = sorting_df.nsmallest(n, "x", keep)

    out = _data.copy()  # attrs copied
    out = out.loc[sorting_df.index, :]  # attrs kept
    return out


@slice_min.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy,
    order_by: Iterable[Any],
    n: int = 1,
    prop: float = None,
    with_ties: Union[bool, str] = True,
) -> DataFrameGroupBy:
    """slice_min for DataFrameGroupBy object"""
    out = _data._datar_apply(
        slice_min,
        order_by=order_by,
        n=n,
        prop=prop,
        with_ties=with_ties,
    )
    return reconstruct_tibble(_data, out)


@slice_min.register(DataFrameRowwise, context=Context.EVAL)
def _(
    _data: DataFrameRowwise,
    order_by: Iterable[Any],
    n: int = 1,
    prop: float = None,
    with_ties: Union[bool, str] = True,
) -> DataFrameRowwise:
    """slice_min for DataFrameRowwise object"""
    out = slice_min.dispatch(DataFrame)(
        ungroup(_data, __calling_env=CallingEnvs.REGULAR),
        order_by=order_by,
        n=n,
        prop=prop,
        with_ties=with_ties,
    )
    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame, extra_contexts={"order_by": Context.EVAL})
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
    n = _n_from_prop(_data.shape[0], n, prop)

    sorting_df = DataFrame(index=_data.index)
    sorting_df["x"] = order_by
    keep = {True: "all", False: "first"}.get(with_ties, with_ties)
    sorting_df = sorting_df.nlargest(n, "x", keep)

    out = _data.copy()  # attrs copied
    out = out.loc[sorting_df.index, :]  # attrs kept
    return out


@slice_max.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy,
    order_by: Iterable[Any],
    n: int = 1,
    prop: float = None,
    with_ties: Union[bool, str] = True,
) -> DataFrameGroupBy:
    """slice_max for DataFrameGroupBy object"""
    out = _data._datar_apply(
        slice_max,
        order_by=order_by,
        n=n,
        prop=prop,
        with_ties=with_ties,
    )
    return reconstruct_tibble(_data, out)


@slice_max.register(DataFrameRowwise, context=Context.EVAL)
def _(
    _data: DataFrameRowwise,
    order_by: Iterable[Any],
    n: int = 1,
    prop: float = None,
    with_ties: Union[bool, str] = True,
) -> DataFrameRowwise:
    """slice_max for DataFrameRowwise object"""
    out = slice_max.dispatch(DataFrame)(
        ungroup(_data, __calling_env=CallingEnvs.REGULAR),
        order_by=order_by,
        n=n,
        prop=prop,
        with_ties=with_ties,
    )
    return reconstruct_tibble(_data, out, keep_rowwise=True)


@register_verb(DataFrame, extra_contexts={"weight_by": Context.EVAL})
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
    n = _n_from_prop(_data.shape[0], n, prop)
    if n == 0:
        # otherwise _data.sample raises error when weight_by is empty as well
        return _data.iloc[[], :]

    return _data.sample(
        n=n,
        replace=replace,
        weights=weight_by,
        random_state=random_state,
        axis=0,
    )


@slice_sample.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy,
    n: int = 1,
    prop: float = None,
    weight_by: Iterable[Union[int, float]] = None,
    replace: bool = False,
    random_state: Any = None,
) -> DataFrameGroupBy:
    out = _data._datar_apply(
        slice_sample,
        n=n,
        prop=prop,
        weight_by=weight_by,
        replace=replace,
        random_state=random_state,
    )
    return reconstruct_tibble(_data, out)


@slice_sample.register(DataFrameRowwise, context=Context.EVAL)
def _(
    _data: DataFrameRowwise,
    n: int = 1,
    prop: float = None,
    weight_by: Iterable[Union[int, float]] = None,
    replace: bool = False,
    random_state: Any = None,
) -> DataFrameRowwise:
    out = slice_sample.dispatch(DataFrame)(
        _data,
        n=n,
        prop=prop,
        weight_by=weight_by,
        replace=replace,
        random_state=random_state,
    )
    return reconstruct_tibble(_data, out, keep_rowwise=True)


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


def _sanitize_rows(rows: Iterable, nrow: int, base0: bool = None) -> List[int]:
    """Sanitize rows passed to slice"""
    rows = Collection(*rows, pool=nrow, base0=base0)
    if rows.error:

        raise rows.error from None
    invalid_type_rows = [
        row
        for row in rows.unmatched
        if not isinstance(row, (int, type(None), type(NA)))
    ]
    if invalid_type_rows:
        raise TypeError("`slice()` expressions should return indices.")
    return unique(rows)
