"""Provide DataFrameGroupBy and DataFrameRowwise"""
from typing import Any, Callable, Mapping, Tuple, Union
from abc import ABC, abstractmethod

import numpy
import pandas
from pandas import DataFrame, Series, RangeIndex

from pipda.function import Function, FastEvalFunction
from pipda.symbolic import DirectRefItem, DirectRefAttr, ReferenceAttr
from pipda.utils import CallingEnvs

from .defaults import DEFAULT_COLUMN_PREFIX
from .types import StringOrIter, is_scalar
from .utils import apply_dtypes
from ..base import setdiff

# pylint: disable=too-many-ancestors,invalid-overridden-method


class DataFrameGroupByABC(DataFrame, ABC):
    """Abstract class for DataFrameGroupBy"""

    def __init__(
        self,
        data: Any,
        _group_vars: StringOrIter = None,
        _group_drop: bool = None,
        # used for copy, etc so we don't need to recompute the group data
        _group_data: DataFrame = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(data, DataFrame):
            kwargs["copy"] = True
        super().__init__(data, **kwargs)
        # drop index to align to tidyverse's APIs
        self.reset_index(drop=True, inplace=True)

        # rowwise
        if _group_vars is None:
            _group_vars = []

        if is_scalar(_group_vars):
            _group_vars = [_group_vars]

        # In order to align to dplyr's API
        self.attrs["_group_drop"] = True if _group_drop is None else _group_drop
        self.attrs["_group_vars"] = _group_vars
        self.attrs["_group_data"] = _group_data

    @abstractmethod
    def _datar_apply(
        self,
        _func: Callable,
        *args: Any,
        _mappings: Mapping[str, Any] = None,
        _method: str = "apply",
        _groupdata: bool = True,
        _drop_index: bool = True,
        **kwargs: Any,
    ) -> DataFrame:
        ...  # pragma: no cover

    def copy(  # pylint: disable=arguments-differ
        self,
        deep: bool = True,
        copy_grouped: bool = False,
    ) -> "DataFrameGroupByABC":
        """Copy the dataframe and keep the class"""
        if not copy_grouped:
            return super().copy(deep)

        if deep:
            return self.__class__(
                super().copy(),
                _group_vars=self.attrs["_group_vars"][:],
                _group_drop=self.attrs["_group_drop"],
                _group_data=self._group_data.copy(),
            )
        # we still need to calculate _grouped_df
        return self.__class__(
            self,
            _group_vars=self.attrs["_group_vars"],
            _group_drop=self.attrs["_group_drop"],
            _group_data=self._group_data,
        )


class DataFrameGroupBy(DataFrameGroupByABC):
    """A customized DataFrameGroupBy class, other than pandas' DataFrameGroupBy

    Pandas' DataFrameGroupBy has obj refer to the original data frame. We do it
    the reverse way by attaching the groupby object to the frame. So that it is:
    1. easier to write single dispatch functions, as DataFrameGroupBy is now a
        subclass of pandas' DataFrame
    2. easier to display the frame. We can use all utilities for frame to
        display. By `core._frame_format_patch.py`, we are also able to show the
        grouping information
    3. possible for future optimizations

    Known Issues:
        - Due to https://github.com/pandas-dev/pandas/issues/35202
            Currently `dropna` is fixed to True of `df.groupby(...)`
            So no NAs will be kept in group vars
        - `_drop = FALSE` does not work when there are multiple group vars
        - Since group vars are required in `DataFrame.groupby()`, so virtual
            groupings are not supported.
        - Groupby on a column with tuples creates a multiindex
            https://github.com/pandas-dev/pandas/issues/21340
        - Order of group data/groups does not follow the categories/levels of
            a category group variable.

    Args:
        data: Data that used to construct the frame.
        **kwargs: Additional keyword arguments passed to DataFrame constructor.
        _group_vars: The grouping variables
        _group_drop: Whether to drop non-observable rows
        _group_data: In most cases, used for copy. Use this groupdata
            if provided to avoid recalculate.

    Attributes:
        _grouped_df: The grouped data frame (pandas' DataFrameGroupBy object)
    """

    def __init__(
        self,
        data: Any,
        _group_vars: StringOrIter = None,
        _group_drop: bool = None,
        _group_data: DataFrame = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(data, _group_vars, _group_drop, _group_data, **kwargs)
        self.__dict__["_grouped_df"] = self.groupby(
            _group_vars,
            dropna=True,
            sort=False,
            observed=self.attrs["_group_drop"],
        )

    # @property
    # def _constructor(self):
    #     return DataFrameGroupBy

    @property
    def _group_data(self):
        """The group data"""
        # compose group data using self._grouped_df.grouper
        if self.attrs["_group_data"] is None:
            self.attrs["_group_data"] = DataFrame(
                (
                    [key] + [list(val)]
                    if len(self.attrs["_group_vars"]) == 1
                    else list(key) + [list(val)]
                    for key, val in self._grouped_df.groups.items()
                ),
                columns=self.attrs["_group_vars"] + ["_rows"],
            )
            apply_dtypes(
                self.attrs["_group_data"],
                self.dtypes[self.attrs["_group_vars"]].to_dict(),
            )

        return self.attrs["_group_data"]

    def _datar_apply(
        self,
        _func: Callable,
        *args: Any,
        _mappings: Mapping[str, Any] = None,
        _method: str = "apply",
        _groupdata: bool = True,
        _drop_index: bool = True,
        **kwargs: Any,
    ) -> DataFrame:
        """Customized apply.

        Aggregation on single columns will be tried to optimized.

        Args:
            _func: The function to be applied.
            *args: The non-keyword arguments for the function
            _mappings: The mapping (new name => transformation/aggregation)
                This will be used to check if we can turn the apply into agg.
                transformations like `f.x.mean()` or `mean(f.x)` will use agg
                to do the aggregation.
                `_func` will be used to apply if any item fails to be optimized.
            _method: Only optimize single column transformations
                when `agg` is provided
            _groupdata: Whether attach the group data to the result data frame
                anyway. Pandas will lose them if is a transform (num. of
                rows of result data frame is equal to the source data frame)
                instead of an aggregation
            _drop_index: Whether we should drop the index when the
                sub data frame goes into the apply function (`_func`).

        Returns:
            The transformed/aggregated data frame
        """
        optms = _optimizable(_mappings)
        if _method == "agg" and optms:
            out = self._datar_agg(optms)

        else:
            group_index = -1

            def _applied(subdf):
                nonlocal group_index
                group_index += 1
                if _drop_index:
                    subdf = subdf.reset_index(drop=True)
                subdf.attrs["_group_index"] = group_index
                subdf.attrs["_group_data"] = self._group_data
                ret = _func(subdf, *args, **kwargs)
                return None if ret is None else ret

            # keep the order
            out = self._grouped_df.apply(_applied).sort_index(level=-1)

        if not _groupdata:
            return out.reset_index(drop=True)

        if (
            self.shape[0] > 0
            and self.shape[0] == out.shape[0]
            and out.index.names == [None]
        ):
            gkeys = self._group_data[
                self._group_data.columns[:-1].difference(out.columns)
            ]
            return pandas.concat([gkeys, out], axis=1)

        # in case the group vars are mutated
        index_to_reset = setdiff(
            self.attrs["_group_vars"],
            out.columns,
            __calling_env=CallingEnvs.REGULAR,
        )
        return out.reset_index(level=index_to_reset).reset_index(drop=True)

    def _datar_agg(
        self,
        _mappings: Mapping[str, Any],
    ) -> DataFrame:
        """Apply aggregation to the mappings"""
        return self._grouped_df.agg(**_mappings)


class DataFrameRowwise(DataFrameGroupBy):
    """Rowwise dataframe"""

    __init__ = DataFrameGroupByABC.__init__

    @property
    def _group_data(self):
        """The group data with the ori"""
        if self.attrs["_group_data"] is None:
            self.attrs["_group_data"] = self[self.attrs["_group_vars"]].assign(
                _rows=[[i] for i in range(self.shape[0])]
            )

        return self.attrs["_group_data"]

    def _datar_apply(
        self,
        _func: Callable,
        *args: Any,
        _mappings: Mapping[str, Any] = None,
        _method: str = "apply",
        _groupdata: bool = True,
        _drop_index: bool = True,
        **kwargs: Any,
    ) -> DataFrame:
        # TODO: check if _func is optimizable.
        def applied(ser, *args, **kwargs):
            """Make sure a series is returned"""
            ret = _func(ser, *args, **kwargs)
            if isinstance(ret, DataFrame) and ret.shape[0] > 1:
                raise ValueError(
                    "Must return a 1d array, a Series or a DataFrame "
                    "with 1 row when a function is applied to "
                    "a DataFrameRowwise object."
                )

            if isinstance(ret, DataFrame):
                ret = ret.iloc[0, :]

            if ret is None:
                ret = []

            if is_scalar(ret):
                ret = [ret]

            if len(ret) == 0:
                ret = Series(ret, dtype=getattr(ret, "dtype", object))
            else:
                ret = Series(ret)

            if isinstance(ret.index, RangeIndex):
                ret.index = (
                    f"{DEFAULT_COLUMN_PREFIX}{i}" for i in range(len(ret))
                )

            return ret

        out = self.apply(
            applied,
            axis=1,
            raw=False,
            # result_type='reduce',
            args=args,
            **kwargs,
        )
        if not _groupdata or not self.attrs["_group_vars"]:
            return out

        return pandas.concat([self[self.attrs["_group_vars"]], out], axis=1)


def _optimizable_func(
    name: str, kwargs: Mapping[str, Any]
) -> Union[str, Callable]:
    """Get the name of the function that can be optimized.

    Currently, only functions avaiable with numpy is able to be optimized

    Args:
        name: The name of the function
        kwargs: The keyword arguments to be passed to the function
            Current, if `na_rm` is `True`, then `nan` version of the function
            will be used

    Return:
        The name of the function that can be optimized.
    """
    if not hasattr(numpy, name):
        return None
    if kwargs.get("na_rm", False):
        return getattr(numpy, f"nan{name}")
    return getattr(numpy, name)


def _optimizable(
    mappings: Mapping[str, Any]
) -> Mapping[str, Tuple[str, Union[str, Callable]]]:
    """Turn mappings (new column name => transform/aggregation) to
    (new column name => tuple(old column name, aggregation function))
    """
    if mappings is None:
        return None

    out = {}
    for key, val in mappings.items():
        # optimize constants?
        if (
            isinstance(val, FastEvalFunction)
            and len(val._pipda_args) == 1
            and isinstance(val._pipda_args[0], (DirectRefItem, DirectRefAttr))
        ):
            # mean(f.x)
            ofun = _optimizable_func(
                val._pipda_func.__name__, val._pipda_kwargs
            )
            if not ofun:
                return None
            out[key] = (val._pipda_args[0]._pipda_ref, ofun)
        elif (
            isinstance(val, Function)
            and isinstance(val._pipda_func, ReferenceAttr)
            and isinstance(
                val._pipda_func._pipda_parent, (DirectRefItem, DirectRefAttr)
            )
        ):
            # f.x.mean()
            out[key] = (
                val._pipda_func._pipda_parent._pipda_ref,
                val._pipda_func._pipda_ref,
            )
        else:
            return None
    return out
