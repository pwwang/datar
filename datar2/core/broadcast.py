"""All broadcasting (value recycling) rules

There are basically 4 places where broadcasting is needed.
1. in `mutate()`
2. in `summarise()`
3. in `tibble()`
4. and arithemetic operators

The base to be used to broadcast other values should be either:
- a Series object (e.g. `f.a` when data is a DataFrame object)
- a DataFrame/Tibble object
    e.g. `f[f.a:]` when data is a DataFrame/Tibble object
- a SeriesGroupBy object (e.g. `f.a` when data is a TibbleGrouped object)
- a TibbleGrouped object (e.g. `f[f.a:]` when data is a TibbleGrouped object)

In `summarise()`, `tibble()` and the operands for arithemetic operators, the
base should also be broadcasted. For example:

>>> tibble(x=[1, 2]) >> group_by(f.x) >> summarise(x=f.x + [1, 2])
>>> # tibble(x=[2, 3, 3, 4])

In the above example, `f.x` is broadcasted into `[1, 1, 2, 2]` based on the
right operand `[1, 2]`
"""

import time
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Tuple, Union

import numpy as np
from pandas import DataFrame, Series
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy, GroupBy
from pandas.api.types import is_scalar

from .tibble import Tibble, TibbleGrouped, TibbleRowwise
from .utils import regcall, name_of

if TYPE_CHECKING:
    from pandas import Index, Grouper

BroadcastingBaseType = Union[DataFrame, Series, SeriesGroupBy]
GroupedType = Union[GroupBy, TibbleGrouped]


def _regroup(x: GroupBy, repeats: Union[int, np.ndarray]) -> GroupBy:
    """Regroup grouped object if some groups get broadcasted"""
    from ..dplyr import group_keys

    gdata = regcall(group_keys, x)
    # Get the data with the new_indices with original grouping variables
    g_indices = x.grouper.group_info[0].repeat(repeats)
    gdata = gdata.take(g_indices)
    grouped = gdata.groupby(x.grouper.names)
    o_indices = np.arange(x.obj.shape[0]).repeat(repeats)
    return x.obj.take(o_indices).groupby(grouped.grouper)


def _agg_result_compatible(index: "Index", grouper: "Grouper") -> bool:
    """Check index of an aggregated result is compatible with a grouper"""
    if index.names != grouper.names:
        return False

    if not index.symmetric_difference(grouper.result_index).empty:
        return False

    # also check the size
    size1 = index.value_counts(sort=False)
    size2 = grouper.size()
    return ((size1 == 1) | (size2 == 1) | (size1 == size2)).all()


def _grouper_compatible(grouper1: "Grouper", grouper2: "Grouper") -> bool:
    """Check if two groupers are compatible"""
    if grouper1 is grouper2:
        return True

    if grouper1.names != grouper2.names:
        return False

    if not grouper1.result_index.symmetric_difference(
        grouper2.result_index
    ).empty:
        return False

    # also check the size
    size1 = grouper1.size()
    size2 = grouper2.size()
    return ((size1 == 1) | (size2 == 1) | (size1 == size2)).all()


@singledispatch
def _broadcast_base(
    value,
    base: BroadcastingBaseType,
    name: str = None,
) -> BroadcastingBaseType:
    """Broadcast the base dataframe when value has more elements

    Args:
        value: The value
        base: The base data frame

    Returns:
        A tuple of the transformed value and base
    """
    # plain arrays, scalars
    if is_scalar(value) or len(value) == 1:
        return base

    name = name or name_of(value)

    if isinstance(base, GroupBy):
        sizes = base.grouper.size()
        usizes = sizes.unique()

        # Broadcast each group into size len(value)
        # usizes should be only 1 number, or [1, len(value)]
        if usizes.size == 0:
            raise ValueError(f"`{name}` must be size [0 1], not {len(value)}.")

        if usizes.size == 1:
            if usizes[0] == 1:
                return _regroup(base, len(value))

            if usizes[0] != len(value):
                raise ValueError(
                    f"Cannot recycle `{name}` with size "
                    f"{len(value)} to {usizes[0]}."
                )
            return base

        if usizes.size == 2:
            if set(usizes) != set([1, len(value)]):
                size_tip = usizes[usizes != len(value)][0]
                raise ValueError(
                    f"Cannot recycle `{name}` with size "
                    f"{len(value)} to {size_tip}."
                )

            if not base.obj.index.is_unique:
                # already broadcasted
                return base

            # broadcast size=1 groups and regroup
            repeats = np.ones(base.obj.shape[0], dtype=int)
            idx_to_broadcast = np.concatenate(
                [base.grouper.indices[i] for i in sizes[sizes == 1].index]
            )
            repeats[idx_to_broadcast] = len(value)
            return _regroup(base, repeats)

        size_tip = usizes[usizes != len(value)][0]
        raise ValueError(
            f"Cannot recycle `{name}` with size {len(value)} to {size_tip}."
        )

    if isinstance(base, TibbleGrouped):
        grouped_old = base._datar["grouped"]
        grouped_new = _broadcast_base(value, grouped_old, name)
        if grouped_new is not grouped_old:
            base = TibbleGrouped.from_groupby(grouped_new, deep=False)
        return base

    # DataFrame/Series
    if not base.index.is_unique:
        return base

    # The length should be [1, len(value)]
    if base.shape[0] == len(value):
        return base

    if base.shape[0] == 1:
        base = base.take(base.index.repeat(len(value)))
        return base

    raise ValueError(
        f"`{name}` must be size [1 {base.shape[0]}], not {len(value)}."
    )


@_broadcast_base.register(GroupBy)
def _(
    value: GroupBy,
    base: BroadcastingBaseType,
    name: str = None,
) -> Tuple[Any, BroadcastingBaseType]:
    """Broadcast grouped object when value is a grouped object"""
    name = name or name_of(value)

    if isinstance(base, GroupBy):

        if not _grouper_compatible(value.grouper, base.grouper):
            raise ValueError(f"`{name}` has an incompatible grouper.")

        if (value.grouper.size() == 1).all():
            # Don't modify base when values are 1-size groups
            # Leave it to broadcast_to() to broadcast to values
            # No need to broadcast the base
            return base

        # check if base has already broadcasted
        if not base.obj.index.is_unique:
            return base

        # Broadcast size-1 groups in base
        base_sizes = base.grouper.size()
        base_sizes1 = base_sizes == 1
        val_sizes = value.grouper.size()
        repeats = np.ones(base.obj.shape[0], dtype=int)
        idx_to_broadcast = np.concatenate(
            [base.grouper.indices[i] for i in base_sizes[base_sizes1].index]
        )
        repeats[idx_to_broadcast] = val_sizes[base_sizes1]
        return _regroup(base, repeats)

    if isinstance(base, TibbleGrouped):
        grouped_old = base._datar["grouped"]
        grouped_new = _broadcast_base(value, grouped_old, name)
        if grouped_new is not grouped_old:
            base = TibbleGrouped.from_groupby(grouped_new, deep=False)
        return base

    # base is ungrouped
    # DataFrame/Series

    # df >> group_by(f.a) >> mutate(new_col=tibble(x=1, y=f.a))
    #                                              ^^^^^^^^^^
    val_sizes = value.grouper.size()

    if (
        base.shape[0] == 1
        or (val_sizes == base.shape[0]).all()
    ):
        if base.shape[0] == 1:
            repeats = value.obj.shape[0]
        else:
            repeats = val_sizes

        base_is_df = isinstance(base, DataFrame)
        base = base.reindex(base.index.repeat(repeats)).groupby(
            value.grouper,
            observed=value.observed,
            sort=value.sort,
            dropna=value.dropna,
        )
        if base_is_df:
            base = TibbleGrouped.from_groupby(base)
        return base

    # Otherwise
    raise ValueError(f"Can't recycle a grouped object `{name}` to ungrouped.")


@_broadcast_base.register(TibbleGrouped)
def _(
    value: TibbleGrouped,
    base: BroadcastingBaseType,
    name: str = None,
) -> BroadcastingBaseType:
    """Broadcast base based on a TibbleGrouped object

    For example, `df >> group_by(f.a) >> mutate(new_col=tibble(x=f.a))`
    """
    return _broadcast_base(value._datar["grouped"], base, name)


@_broadcast_base.register(DataFrame)
@_broadcast_base.register(Series)
def _(
    value: Union[DataFrame, Series],
    base: BroadcastingBaseType,
    name: str = None,
) -> Tuple[Any, BroadcastingBaseType]:
    """Broadcast a DataFrame/Series object to a grouped object

    This is mostly a case when trying to broadcast an aggregated object to
    the original object. For example: `gf >> mutate(f.x / sum(f.x))`

    But `sum(f.x)` could return a Series object that has more than 1 elements
    for a group. Then we need to broadcast `f.x` to match the result.
    """
    if isinstance(base, GroupBy):
        # Now the index of value works more like grouping data
        if not _agg_result_compatible(value.index, base.grouper):
            name = name or name_of(value)
            raise ValueError(f"`{name}` is an incompatible aggregated result.")

        val_sizes = value.index.value_counts(sort=False)
        if (val_sizes == 1).all():
            # Don't modify base when values are 1-size groups
            # Leave it to broadcast_to() to broadcast to values
            # No need to broadcast the base
            return base

        if not base.obj.index.is_unique:
            return base

        # Broadcast size-1 groups in base
        base_sizes = base.grouper.size()
        base_sizes1 = base_sizes == 1
        repeats = np.ones(base.obj.shape[0], dtype=int)
        idx_to_broadcast = np.concatenate(
            [base.grouper.indices[i] for i in base_sizes[base_sizes1].index]
        )
        repeats[idx_to_broadcast] = val_sizes[base_sizes1]
        return _regroup(base, repeats)

    if isinstance(base, TibbleGrouped):
        grouped_old = base._datar["grouped"]
        grouped_new = _broadcast_base(value, grouped_old, name)
        if grouped_new is not grouped_old:
            base = TibbleGrouped.from_groupby(grouped_new, deep=False)
        return base

    # base: DataFrame/Series
    if not base.index.is_unique:
        return base

    return base.reindex(value.index)


@singledispatch
def broadcast_to(
    value,
    index: "Index",
    grouper: "Grouper" = None,
) -> Series:
    """Broastcast value to expected dimension, the result is a series with
    the given index

    Before calling this function, make sure that the index is already
    broadcasted based on the value. This means that index is always the wider
    then the value if it has one. Also the size for each group is larger then
    the length of the value if value doesn't have an index.

    Examples:
        >>> # 1. broadcast scalars/arrays(no index)
        >>> broadcast_to(1, Index(range(3)))
        >>> # Series: [1,1,1], index: range(3)
        >>> broadcast_to([1, 2], Index([0, 0, 1, 1]))
        >>> # ValueError
        >>>
        >>> # grouper: size()        # [1, 1]
        >>> # grouper: result_index  # ['a', 'b']
        >>> broadcast_to([1, 2], Index([0, 0, 1, 1]), grouper)
        >>> # Series: [1, 2, 1, 2], index: [0, 0, 1, 1]
        >>>
        >>> # 2. broadcast a Series
        >>> broadcast(Series([1, 2]), index=[0, 0, 1, 1])
        >>> # Series: [1, 1, 2, 2], index: [0, 0, 1, 1]
        >>> broadcast(
        >>>     Series([1, 2], index=['a', 'b']),
        >>>     index=[4, 4, 5, 5],
        >>>     grouper,
        >>> )
        >>> # Series: [1, 1, 2, 2], index: [4, 4, 5, 5]

    Args:
        value: Value to be broadcasted
        index: The index to broadcasted to
        grouper: The grouper of the original value

    Returns:
        The series with the given index
    """
    if is_scalar(value) or len(value) == 1:
        return value

    if not grouper:
        if len(value) == 0:
            return Series(value, index=index, dtype=object)

        # Series will raise the length problem
        return Series(value, index=index)

    gsizes = grouper.size()
    if gsizes.size == 0:
        return Series(value, index=index)

    # broadcast value to each group
    # length of each group is checked in _broadcast_base
    return Series(np.tile(value, grouper.ngroups), index=index)


@broadcast_to.register(DataFrame)
@broadcast_to.register(Series)
def _(
    value: Union[DataFrame, Series],
    index: "Index",
    grouper: "Grouper" = None,
) -> Union[Tibble, Series]:
    """Broadcast series"""
    if not grouper:
        if isinstance(value, Series):
            return Series(value, index=index, name=value.name)

        return Tibble(value, index=index)

    # now target is grouped and the value's index is overlapping with the
    # grouper's index
    # This is typically an aggregated result to the orignal structure
    # For example:  f.x.mean() / f.x
    if isinstance(value, Series):
        out = Series(
            value,
            index=grouper.result_index.take(grouper.group_info[0]),
            name=value.name,
        )
    else:  # DataFrame
        out = Tibble(
            value,
            index=grouper.result_index.take(grouper.group_info[0]),
        )

    out.index = index
    return out


@broadcast_to.register(SeriesGroupBy)
@broadcast_to.register(DataFrameGroupBy)
def _(
    value: Union[SeriesGroupBy, DataFrameGroupBy],
    index: "Index",
    grouper: "Grouper" = None,
) -> Union[Series, Tibble]:
    """Broadcast pandas grouped object"""
    if not grouper:
        raise ValueError(
            "Cann't broadcast grouped object to a non-grouped object."
        )

    # Compatibility has been checked in _broadcast_base
    if isinstance(value, SeriesGroupBy):
        return Series(value.obj, index=index, name=value.obj.name)

    return Tibble(value.obj, index=index)


@broadcast_to.register(TibbleGrouped)
def _(
    value: TibbleGrouped,
    index: "Index",
    grouper: "Grouper" = None,
) -> Tibble:
    """Broadcast TibbleGrouped object"""
    return broadcast_to(
        value._datar["grouped"],
        index=index,
        grouper=grouper,
    )


@singledispatch
def broadcast2(left, right) -> Tuple[Any, Any, "Grouper", bool]:
    """Broadcast 2 values for operators"""
    # scalar or arrays
    if isinstance(
        right,
        (
            DataFrameGroupBy,
            SeriesGroupBy,
            DataFrame,
            Series,
        ),
    ):
        right, left, grouper, is_rowwise = broadcast2(right, left)
        return left, right, grouper, is_rowwise

    return left, right, None, False


@broadcast2.register(TibbleGrouped)
def _(left: TibbleGrouped, right: Any) -> Tuple[Any, Any, "Grouper", bool]:
    is_rowwise = isinstance(left, TibbleRowwise)
    grouper = left._datar["grouped"].grouper
    if isinstance(right, TibbleGrouped):
        is_rowwise = is_rowwise and isinstance(right, TibbleRowwise)
    elif isinstance(right, SeriesGroupBy):
        is_rowwise = is_rowwise and getattr(right, "is_rowwise", False)

    right = broadcast_to(right, left.index, grouper)
    return left, right, grouper, is_rowwise


@broadcast2.register(DataFrameGroupBy)
@broadcast2.register(SeriesGroupBy)
def _(
    left: Union[DataFrameGroupBy, SeriesGroupBy],
    right: Any,
) -> Tuple[Any, Any, "Grouper", bool]:
    is_rowwise = getattr(left, "is_rowwise", False)
    grouper = left.grouper
    if isinstance(right, TibbleGrouped):
        is_rowwise = is_rowwise and isinstance(right, TibbleRowwise)
    elif isinstance(right, SeriesGroupBy):
        is_rowwise = is_rowwise and getattr(right, "is_rowwise", False)

    right = broadcast_to(right, left.obj.index, grouper)
    return left.obj, right, grouper, is_rowwise


@broadcast2.register(DataFrame)
@broadcast2.register(Series)
def _(
    left: Union[DataFrame, Series],
    right: Any,
) -> Tuple[Any, Any, "Grouper", bool]:
    if isinstance(
        right,
        (
            DataFrameGroupBy,
            SeriesGroupBy,
            TibbleGrouped,
        ),
    ):
        right, left, grouper, is_rowwise = broadcast2(right, left)
        return left, right, grouper, is_rowwise

    if isinstance(right, (DataFrame, Series)):
        if right.size < left.size:
            right = broadcast_to(right, left.index)
        else:
            left = broadcast_to(left, right.index)

    else:
        right = broadcast_to(right, left.index)

    return left, right, None, False


@singledispatch
def init_tibble_from(value, name: str) -> Tibble:
    """Initialize a tibble from a value"""
    if is_scalar(value):
        return Tibble({name: [value]})

    return Tibble({name: value})


@init_tibble_from.register(Series)
def _(value: Series, name: str) -> Tibble:
    name = name or value.name
    return Tibble({name: value})


@init_tibble_from.register(SeriesGroupBy)
def _(value: SeriesGroupBy, name: str) -> Tibble:
    name = name or value.obj.name
    if getattr(value, "is_rowwise", False):
        return TibbleRowwise.from_groupby(value, name)
    return TibbleGrouped.from_groupby(value, name)


@init_tibble_from.register(DataFrame)
@init_tibble_from.register(DataFrameGroupBy)
def _(value: Union[DataFrame, DataFrameGroupBy], name: str) -> Tibble:
    from ..tibble import as_tibble

    result = as_tibble(value)
    if name:
        result.columns = [f"{name}${col}" for col in result.columns]
    return result


def add_to_tibble(
    tbl: Tibble,
    name: str,
    value: Any,
    allow_dup_names: bool = False,
    broadcast_tbl: bool = False,
) -> "Tibble":
    """Add data to tibble"""
    if value is None:
        return tbl

    if tbl is None:
        return init_tibble_from(value, name)

    if broadcast_tbl:
        tbl = _broadcast_base(value, tbl, name)

    if name is None and isinstance(value, DataFrame):
        for col in value.columns:
            tbl = add_to_tibble(tbl, col, value[col], allow_dup_names)

        return tbl

    if not allow_dup_names or name not in tbl:
        tbl[name] = value
    else:
        # better way to add a column with duplicated name?
        columns = tbl.columns.values.copy()
        dupcol_idx = tbl.columns.get_indexer_for([name])
        columns[dupcol_idx] = f"{name}_{int(time.time())}"
        tbl.columns = columns
        tbl[name] = value
        columns[dupcol_idx] = name
        tbl.columns = columns.tolist() + [name]

    return tbl
