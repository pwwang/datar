"""Provide DataFrameGroupBy and DataFrameRowwise"""
from typing import Any, Callable

from pandas import DataFrame
from pandas.util._decorators import cache_readonly

from .types import StringOrIter, is_scalar
# pylint: skip-file
class DataFrameGroupBy(DataFrame):
    """A customized DataFrameGroupBy class, other than pandas' DataFrameGroupBy

    Pandas' DataFrameGroupBy has obj refer to the original data frame. We do it
    the reverse way by attaching the groupby object to the frame. So that it is:
    1. easier to write single dispatch functions, as DataFrameGroupBy is now a
        subclass of pandas' DataFrame
    2. easier to display the frame. We can use all utilities for frame to
        display. By `core._frame_format_patch.py`, we are also able to show the
        grouping information

    Args:
        data: Data that used to construct the frame.
        **kwargs: Additional keyword arguments passed to DataFrame constructor.
        _group_vars: The grouping variables
        _group_drop: Whether to drop non-observable rows
        _group_data: Reuse other group data so we don't re-compute
            The `_group_data` should match the one computed by `_group_vars`.
            Should be use this given `_group_data` to construct `grouper`, or
            use the pandas way (`DataFrame.groupby()`)?
            This is mostly for internal use.
    """
    def __init__(
        self,
        data: Any,
        _group_vars: StringOrIter = None,
        _group_drop: bool = None,
        _group_data: DataFrame = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(data, DataFrame):
            kwargs["copy"] = True
        super().__init__(data, **kwargs)
        # drop index to align to tidyverse's APIs
        self.reset_index(drop=True, inplace=True)

        if is_scalar(_group_vars):
            _group_vars = [_group_vars]

        # align to dplyr's API
        self.__dict__["_group_meta"] = {
            "_group_drop": _group_drop,
            "_group_vars": _group_vars,
            "_group_data": _group_data,
        }

        self.__dict__["_grouped_df"] = self.groupby(_group_vars)

    @cache_readonly
    def _group_drop(self):
        """A flag whether to drop the non-observable rows"""
        out = self._group_meta['_group_drop']
        return True if out is None else out

    @cache_readonly
    def _group_vars(self):
        return self._group_meta['_group_vars']

    @cache_readonly
    def _group_data(self):
        if self._group_meta['_group_data'] is not None:
            return self._group_meta['_group_data']
        # compose group data using self._grouped_df.grouper

    def _datar_apply(
        self,
        func: Callable,
        mapping: Mapping[str, Expression] = None,
        method: str = 'apply',
        _groupdata: bool = True,
        _drop_index: bool = True,
        **kwargs: Any
    ) -> DataFrame:
        ...
