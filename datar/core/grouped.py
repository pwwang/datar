"""Provide DataFrameGroupBy and DataFrameRowwise"""
from typing import Any, Union
from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy
from pandas._libs import lib
from pandas.core import common as com
from pandas.core.dtypes.common import is_hashable, is_iterator
from pandas.core.indexes.multi import MultiIndex

from .types import StringOrIter


class DatarGroupBy(DataFrame):
    """A DataFrameGroupBy wrapper with

    1. classmethod `from_grouped()` to make a clone from another
    `DataFrameGroupBy` object
    2. attributes `_html_footer` and `_str_footer` for `pdtypes` to show
    additional information
    3. attribute `_group_vars` is added to keep after `summarise()` for
    rowwise data frame. If not rowwise, they are `grouper.names`
    4. getattr()/getitem() returns a SeriesGroupBy object
    """

    @property
    def _constructor(self):
        return DatarGroupBy

    @classmethod
    def from_grouped(
        cls,
        grouped: Union[DataFrameGroupBy, "DatarGroupBy"],
        group_vars: StringOrIter = None
    ) -> "DatarGroupBy":
        """Initiate a DatarGroupBy object"""
        if isinstance(grouped, DataFrameGroupBy):
            out = cls(grouped.obj)
            out.attrs["_grouped"] = grouped
            out.attrs["_group_vars"] = group_vars or [
                name for name in grouped.grouper.names
                if name is not None
            ]
            out.attrs["_html_footer"] = (
                f"<p>Groups: {', '.join(out.attrs['_group_vars'])} "
                f"(n={grouped.grouper.ngroups})</p>"
            )
            out.attrs["_str_footer"] = (
                f"[Groups: {', '.join(out.attrs['_group_vars'])} "
                f"(n={grouped.grouper.ngroups})]"
            )
        else:
            out = cls(grouped)
            out.attrs["_grouped"] = grouped.attrs["_grouped"]
            out.attrs["_group_vars"] = grouped.attrs["_group_vars"]
            out.attrs["_html_footer"] = grouped.attrs["_html_footer"]
            out.attrs["_str_footer"] = grouped.attrs["_str_footer"]

        return out

    def copy(self, deep: bool = True) -> "DatarGroupBy":
        """Make a copy of DatarGroupBy object

        If deep is True, also copy the grouped object (use the grouper object
        to redo the groupby)
        """
        if not deep:
            # attrs also copyied
            return self.__class__.from_grouped(self)

        out = self.attrs["_grouped"].obj.copy()
        out = out.groupby(
            self.attrs["_grouped"].grouper,
            observed=self.attrs["_grouped"].observed,
            sort=self.attrs["_grouped"].sort,
        )
        return self.__class__.from_grouped(out, self.attrs["_group_vars"][:])

    def __getitem__(self, key: Any) -> Any:
        """Make sure df.x or df['x'] returns SeriesGroupBy instead of a Series
        """
        key = lib.item_from_zerodim(key)
        key = com.apply_if_callable(key, self)

        if is_hashable(key) and not is_iterator(key):
            if self.columns.is_unique and key in self.columns:
                assert not isinstance(self.columns, MultiIndex)
                return self.attrs["_grouped"][key]

        return super().__getitem__(key)


class DatarRowwise(DatarGroupBy):
    """Rowwise data frame"""

    @property
    def _constructor(self):
        return DatarRowwise

    @classmethod
    def from_grouped(
        cls,
        grouped: Union[DataFrameGroupBy, "DatarRowwise"],
        group_vars: StringOrIter = None
    ) -> "DatarGroupBy":
        """Initiate a DatarGroupBy object"""
        if isinstance(grouped, DataFrameGroupBy):
            out = cls(grouped.obj)
            out.attrs["_grouped"] = grouped
            out.attrs["_group_vars"] = group_vars or []
            out.attrs["_html_footer"] = (
                f"<p>Rowwise: {', '.join(out.attrs['_group_vars'])} "
                f"(n={out.shape[0]})</p>"
            )
            out.attrs["_str_footer"] = (
                f"[Rowwise: {', '.join(out.attrs['_group_vars'])} "
                f"(n={out.shape[0]})]"
            )
        else:
            out = super().from_grouped(grouped, group_vars)

        return out
