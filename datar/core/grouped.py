"""Provide DataFrameGroupBy class"""
from typing import Any, Callable, List, Mapping
from itertools import product

import pandas
from pandas import DataFrame, Series, Index
from pandas.core.groupby.grouper import Grouping

from .utils import fillna_safe, na_if_safe
from .types import Dtype, is_categorical

# pylint: disable=too-many-ancestors
# pylint: disable=too-many-branches


class DataFrameGroupBy(DataFrame):
    """GroupBy data frame, instead of the one from pandas

    Because
    1. Pandas DataFrameGroupBy cannot handle mutilpe categorical columns as
        groupby variables with non-obserable values
    2. It is very hard to retrieve group indices and data when doing apply
    3. NAs unmatched in grouping variables

    Args:
        data: and
        **kwargs: The values used to pass to the original constructor to
            construct the data frame
        _group_vars: The grouping variables
        _group_drop: Whether to drop non-observable rows
        _group_data: Reuse other group data so we don't re-compute
    """

    def __init__(
        self,
        data: Any,
        _group_vars: List[str] = None,
        _group_drop: bool = None,
        _group_data: DataFrame = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(data, DataFrame):
            kwargs["copy"] = True
        super().__init__(data, **kwargs)
        # drop index to align to tidyverse's APIs
        self.reset_index(drop=True, inplace=True)

        if _group_drop is None:
            _group_drop = self.attrs.get("_group_drop", True)
        self.attrs["_group_drop"] = _group_drop

        if _group_vars is None:
            _group_vars = []
        _group_vars = list(_group_vars)
        self.__dict__["_group_vars"] = _group_vars
        if _group_data is None:
            self.__dict__["_group_data"] = self._compute_groups()
        else:
            self.__dict__["_group_data"] = _group_data

    def _compute_groups(self):
        """Compute group data"""
        # group by values have to be hashable
        if not self._group_vars:
            return DataFrame({"_rows": [list(range(self.shape[0]))]})

        if len(self._group_vars) == 1:
            return self._compute_single_var_groups()

        return self._compute_multiple_var_groups()

    def _compute_single_var_groups(self):
        """Compute groups for a single variable

        NAs are always exluded in pandas groupby operation
        We replace NAs with NA_REPR, do the grouping and then replace it back
        """
        var = self.loc[:, self._group_vars[0]]
        dtype = var.dtype
        var = fillna_safe(var)
        groups = Grouping(
            self.index, var, sort=False, observed=self.attrs["_group_drop"]
        ).groups

        return _groups_to_group_data(
            groups, dtypes={self._group_vars[0]: dtype}, na_if=True
        )

    def _compute_multiple_var_groups(self):
        """Compute groups for multiple vars"""
        from ..base import unique

        dtypes = {}
        groupings = []
        for gvar in self._group_vars:
            dtypes[gvar] = self[gvar].dtype
            groupings.append(
                Grouping(
                    self.index,
                    self[gvar],
                    sort=False,
                    observed=self.attrs["_group_drop"],
                )
            )

        to_groupby = zip(
            *(
                # grouper renamed to grouping_vector in future version of pandas
                getattr(ping, "grouping_vector", getattr(ping, "grouper", None))
                for ping in groupings
            )
        )
        # tupleize_cols = False
        # makes nan equals
        index = Index(to_groupby, tupleize_cols=False)
        groups = self.index.groupby(index)
        # pandas not including unused categories for multiple variables
        # even with observed=False
        # #
        if not self.attrs["_group_drop"]:
            # unobserved = [
            #     self[gvar].values.categories.difference(self[gvar])
            #     if is_categorical(dtype)
            #     else []
            #     for gvar, dtype in dtypes.items()
            # ]
            # maxlen = max((len(unobs) for unobs in unobserved))
            # if maxlen > 0:
            #     unobserved = [
            #         ## Simply adding NAs would change dtype
            #         [NA] if len(unobs) == 0 else unobs
            #         for unobs in unobserved
            #     ]
            #     for row in product(*unobserved):
            #         groups[row] = []
            unobserved = []
            insert_unobs = False
            for gvar, dtype in dtypes.items():
                if is_categorical(dtype):
                    unobs = self[gvar].values.categories.difference(self[gvar])
                    if len(unobs) > 0:
                        unobserved.append(unobs)
                        insert_unobs = True
                    else:
                        unobserved.append(unique(self[gvar]))
                else:
                    unobserved.append(unique(self[gvar]))
            if insert_unobs:
                for row in product(*unobserved):
                    groups[row] = []

        return _groups_to_group_data(groups, dtypes=dtypes)

    def datar_apply(
        self,
        func: Callable,
        *args: Any,
        _groupdata: bool = True,
        _drop_index: bool = True,
        _spike_groupdata: DataFrame = None,
        **kwargs: Any,
    ) -> DataFrame:
        """Apply function to each group

        Args:
            func: The function applied to the groups
            *args, **kwargs: The arguments for func
            _groupdata: Whether to include the group data in the results or not
            _drop_index: Should we drop the index or keep it
            _spike_groupdata: External group data to used, instead of
                `self._group_data`. This is useful for rowwise dataframe when
                no group vars not specified (`self._group_data` has only
                `_rows` column).
        """
        groupdata = (
            self._group_data if _spike_groupdata is None else _spike_groupdata
        )

        def apply_func(gdata_row):
            index = gdata_row[0]
            subdf = self.iloc[gdata_row[1]["_rows"], :]
            if _drop_index:
                subdf.reset_index(drop=True, inplace=True)
            subdf.attrs["_group_index"] = index
            subdf.attrs["_group_data"] = groupdata
            ret = func(subdf, *args, **kwargs)
            if ret is None:
                return ret

            if _groupdata:
                # attaching grouping data
                gdata = groupdata.loc[
                    [index] * ret.shape[0],
                    [gvar for gvar in self._group_vars if gvar not in ret],
                ]
                gdata.index = ret.index
                ret = pandas.concat([gdata, ret], axis=1)
            return ret

        to_concat = [apply_func(row) for row in groupdata.iterrows()]
        if all(elem is None for elem in to_concat):
            return None
        ret = pandas.concat(to_concat, axis=0)
        if _drop_index:
            ret.reset_index(drop=True, inplace=True)

        ret.attrs["_group_drop"] = self.attrs["_group_drop"]
        return ret

    def copy(self, deep: bool = True) -> "DataFrameGroupBy":
        """Copy the dataframe and keep the class"""
        return DataFrameGroupBy(
            super().copy() if deep else self,
            _group_vars=self._group_vars,
            _group_drop=self.attrs.get("_group_drop", True),
            _group_data=self._group_data.copy() if deep else self._group_data,
        )


class DataFrameRowwise(DataFrameGroupBy):
    """Group data frame rowwisely"""

    def _compute_groups(self):
        _rows = Series(
            [[i] for i in range(self.shape[0])], name="_rows", dtype=object
        )
        if not self._group_vars:
            return _rows.to_frame()

        gdata = self[self._group_vars].copy()
        gdata["_rows"] = _rows
        return gdata

    def datar_apply(
        self,
        func: Callable,
        *args: Any,
        _groupdata: bool = True,
        _drop_index: bool = True,
        _spike_groupdata: DataFrame = None,
        **kwargs: Any,
    ) -> DataFrame:
        if _spike_groupdata is not None:
            raise ValueError("`_spike_groupdata` not allowed.")
        if self._group_vars:
            gdata = self._group_data
        else:
            gdata = self.copy()
            gdata["_rows"] = self._group_data

        return super().datar_apply(
            func,
            *args,
            _groupdata=_groupdata,
            _drop_index=_drop_index,
            _spike_groupdata=gdata,
            **kwargs,
        )

    def copy(self, deep: bool = True) -> "DataFrameRowwise":
        return DataFrameRowwise(
            DataFrame.copy(self) if deep else self,
            _group_vars=self._group_vars,
            _group_drop=self.attrs.get("_group_drop", True),
            _group_data=self._group_data.copy() if deep else self._group_data,
        )


def _groups_to_group_data(
    groups: Mapping[Any, Index],
    dtypes: Mapping[str, Dtype],
    na_if: bool = False,
) -> DataFrame:
    """Convert pandas groups dict to group data

    From
        >>> {1: [0], 2: [1]}
    To
        >>> DataFrame({'a': [1, [0]], 'b': [2, [1]]})
    """
    out = DataFrame(
        [(grp,) if len(dtypes) == 1 else grp for grp in groups],
        columns=list(dtypes),
    )

    for gvar, dtype in dtypes.items():
        if na_if:
            out[gvar] = na_if_safe(out[gvar], dtype=dtype)
        else:
            try:
                same_dtype = out[gvar].dtype == dtype
            except TypeError:  # pragma: no cover
                # Cannot interpret 'CategoricalDtype(categories=[1, 2],
                # ordered=False)' as a data type
                same_dtype = False
            if not same_dtype:
                out[gvar] = out[gvar].astype(dtype)

    out["_rows"] = Series(
        [list(rows) for rows in groups.values()],
        dtype=object,  # get rid of numpy warning
    )
    return out.sort_values(list(dtypes)).reset_index(drop=True)
