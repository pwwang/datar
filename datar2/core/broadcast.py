import time
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Tuple, Union

import numpy as np
from pandas import DataFrame, RangeIndex, Series
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy
from pandas.api.types import is_scalar

from datar2.tibble.tibble import as_tibble

from .tibble import Tibble, TibbleGroupby, TibbleRowwise

if TYPE_CHECKING:
    from pandas import Index, Grouper


@singledispatch
def broadcast_self(value, selfobj: Tibble):
    # plain arrays, scalars
    if is_scalar(value) or len(value) == 1:
        return value, selfobj

    if not selfobj.index.is_unique:
        return value, selfobj

    if isinstance(selfobj, TibbleGroupby):
        selfobj = selfobj.reindex(selfobj.index.repeat(len(value)))
        return value, selfobj

    if (
        isinstance(selfobj.index, RangeIndex)
        and (
            selfobj.index.size > 1
            or (selfobj.index.size == 1 and selfobj.index[0] != 0)
        )
    ):
        return value, selfobj

    newval = np.tile(value, selfobj.index.size)
    selfobj = selfobj.reindex(selfobj.index.repeat(len(value)))
    return newval, selfobj


@broadcast_self.register(DataFrameGroupBy)
@broadcast_self.register(SeriesGroupBy)
def _(value: Union[DataFrameGroupBy, SeriesGroupBy], selfobj: Tibble):
    if not selfobj.index.is_unique:
        return value, selfobj

    if not isinstance(selfobj, TibbleGroupby):
        # Cannot broadcast grouped to ungrouped
        # Let broadcast_to raise
        return value, selfobj

    # Check if grouper is compatible
    if not selfobj._datar_meta["grouped"].grouper.result_index.equals(
        value.grouper.result_index
    ):
        raise ValueError(
            "Cannot broadcast between objects with incompatible groupers."
        )

    selfobj = selfobj.reindex(selfobj.index.repeat(value.grouper.size()))
    return value, selfobj


@broadcast_self.register(DataFrame)
@broadcast_self.register(Series)
def _(value: Union[DataFrame, Series], selfobj: Tibble):
    if not selfobj.index.is_unique:
        return value, selfobj

    if isinstance(selfobj, TibbleGroupby):
        # index must match result_index
        val_uindex = value.index.unique()
        if not selfobj._datar_meta["grouped"].grouper.result_index.equals(
            val_uindex
        ):
            raise ValueError(
                "Incompatible summary results."
            )

        if value.index.is_unique:
            return value, selfobj

        selfobj = selfobj.reindex(
            selfobj.index.repeat(value.index.value_counts(sort=False))
        )
        return value, selfobj

    selfobj = selfobj.reindex(value.index)
    return value, selfobj


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
    if is_scalar(value):
        return value

    if not grouper:
        if len(value) == 0:
            return Series(value, index=index, dtype=object)

        return Series(value, index=index)

    # broadcast value to each group
    # length of each group is checked in broadcast_self
    return Series(np.tile(value, grouper.ngroups), index)


@broadcast_to.register(DataFrame)
@broadcast_to.register(Series)
def _(
    value: Union[DataFrame, Series],
    index: "Index",
    grouper: "Grouper" = None,
) -> Union[Tibble, Series]:
    """Broadcast series"""
    if (
        not grouper
        or isinstance(value.index, RangeIndex)
        or not value.index.unique().equals(grouper.result_index)
    ):
        # If the target is not grouped, just broadcast using index
        # Or, if value's index has nothing to do with the grouping
        # also use the index to broadcast

        # also if it is just one-row, one-element
        # broadcast the values
        if isinstance(value.index, RangeIndex) and value.index.size == 1:
            value = value.reindex(value.index.repeat(index.size))
            value.index = index

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
            index=grouper.result_index.repeat(grouper.size()),
            name=value.name,
        )
        out.index = index
        return out

    return Tibble(value, index=index)


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

    # Compatibility has been checked in broadcast_self
    #     raise ValueError(
    #         "Cann't broadcast grouped object to a object with "
    #         "a different grouper."
    #     )

    if isinstance(value, SeriesGroupBy):
        return Series(value.obj, index=index, name=value.obj.name)

    return Tibble(value.obj, index=index)


@broadcast_to.register(TibbleGroupby)
def _(
    value: TibbleGroupby,
    index: "Index",
    grouper: "Grouper" = None,
) -> Tibble:
    """Broadcast TibbleGroupby object"""
    return broadcast_to(
        value._datar_meta["grouped"],
        index=index,
        grouper=grouper,
    )


@singledispatch
def broadcast2(left, right) -> Tuple[Any, Any, "Grouper", bool]:
    """Broadcast 2 values for operators"""
    # scalar or arrays
    if isinstance(right, (
        DataFrameGroupBy,
        SeriesGroupBy,
        DataFrame,
        Series,
    )):
        right, left, grouper, is_rowwise = broadcast2(right, left)
        return left, right, grouper, is_rowwise

    return left, right, None, False


@broadcast2.register(TibbleGroupby)
def _(left: TibbleGroupby, right: Any) -> Tuple[Any, Any, "Grouper", bool]:
    is_rowwise = isinstance(left, TibbleRowwise)
    grouper = left._datar_meta["grouped"].grouper
    if isinstance(right, TibbleGroupby):
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
    if isinstance(right, TibbleGroupby):
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
    if isinstance(right, (
        DataFrameGroupBy,
        SeriesGroupBy,
        TibbleGroupby,
    )):
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
        return TibbleRowwise.from_grouped(value, name)
    return TibbleGroupby.from_grouped(value, name)


@init_tibble_from.register(DataFrame)
@init_tibble_from.register(DataFrameGroupBy)
def _(value: Union[DataFrame, DataFrameGroupBy], name: str) -> Tibble:
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
        value, tbl = broadcast_self(value, tbl)

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
