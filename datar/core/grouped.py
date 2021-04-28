"""Provide DataFrameGroupBy class"""
from typing import Any, Callable, List, Optional
from itertools import product
from collections import defaultdict

import pandas
from pandas.core.arrays.categorical import Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from pandas import DataFrame, Series

from ..base.constants import NA

class DataFrameGroupBy(DataFrame): # pylint: disable=too-many-ancestors
    """GroupBy data frame, instead of the one from pandas

    Because
    1. Pandas DataFrameGroupBy cannot handle mutilpe categorical columns as
        groupby variables with non-obserable values
    2. It is very hard to retrieve group indices and data when doing apply

    Args:
        *args: and
        **kwargs: The values used to construct the data frame
        _group_vars: The grouping variables
        _drop: Whether to drop non-observable rows
        _group_data: Reuse other group data so we don't re-compute
    """
    def __init__(
            self,
            *args: Any,
            _group_vars: Optional[List[str]] = None,
            _drop: Optional[bool] = None,
            _group_data: Optional[DataFrame] = None,
            **kwargs: Any
    ) -> None:
        # drop args[0]'s index as well, if it is a DataFrame.
        # self.reset_index(drop=True, inplace=True)
        if args and not kwargs and isinstance(args[0], DataFrame):
            super().__init__(args[0].copy())
        else:
            super().__init__(*args, **kwargs)
        self.reset_index(drop=True, inplace=True)

        if _drop is None:
            _drop = self.attrs.get('groupby_drop', True)
        self.attrs['groupby_drop'] = _drop

        self.__dict__['_group_vars'] = _group_vars or []
        if _group_data is None:
            self.__dict__['_group_data'] = self._compute_group_data()
        else:
            self.__dict__['_group_data'] = _group_data

    @classmethod
    def construct_from(
            cls,
            data: DataFrame,
            template: "DataFrameGroupBy"
    ) -> "DataFrameGroupBy":
        """Construct from a template dataframe"""
        return cls(
            data,
            _group_vars=template._group_vars,
            _drop=template.attrs.get('groupby_drop', True),
            _group_data=template._group_data
        )

    def _compute_group_data(self): # pylint: disable=too-many-branches
        """Compute group data"""
        # group by values have to be hashable
        if not self._group_vars:
            return DataFrame({
                '_rows': [list(range(self.shape[0]))]
            })

        gdata = self[self._group_vars].sort_values(self._group_vars)
        if self.attrs['groupby_drop']:
            rows_dict = defaultdict(lambda: [])
            for row in gdata.iterrows():
                # NAs not equal
                elems = tuple(
                    None if pandas.isna(elem) else elem
                    for elem in row[1]
                )
                rows_dict[elems].append(row[0])
        else:
            vardata_list = []
            # make sure the value and the value of left column are observed
            # if current column is NA
            var_prevs = [set()]
            var_hasnas = []
            # Try to put NA's last
            # and use the categories if data is Categorical
            for i, var in enumerate(self._group_vars):
                vardata = gdata[var]
                has_na = vardata.isna().any()
                var_hasnas.append(has_na)
                if i > 0:
                    var_prevs.append(set(
                        gdata[vardata.isna()][self._group_vars[i-1]]
                    ))

                if is_categorical_dtype(vardata):
                    vardata = vardata.cat.categories

                vardata = vardata[~pandas.isna(vardata)].unique()

                if has_na:
                    vardata = vardata.tolist() + [NA]
                vardata_list.append(vardata)

            rows_dict = {}
            for row in product(*vardata_list):
                row_pass = True
                for i, elem in enumerate(row):
                    if i == 0 or not pandas.isna(elem):
                        continue
                    # require previous elems to be observed or NA
                    for x in range(i):
                        if (
                                (var_hasnas[x] and not pandas.isna(row[x])) or
                                (
                                    not var_hasnas[x] and
                                    row[x] not in var_prevs[x+1]
                                )
                        ):
                            row_pass = False
                            break

                if row_pass:
                    rows_dict[row] = []

            for row in gdata.itertuples():
                rows_dict[row[1:]].append(row.Index)

        # sort the rows to try to keep the order in original data
        for key, val in rows_dict.items():
            rows_dict[key] = sorted(val)

        ret = DataFrame(
            rows_dict.keys(),
            columns=self._group_vars
        )
        for gvar in self._group_vars:
            if is_categorical_dtype(self[gvar]):
                ret[gvar] = Categorical(
                    ret[gvar],
                    categories=self[gvar].cat.categories
                )
        ret['_rows'] = Series(rows_dict.values(), dtype=object)
        return ret

    def group_apply(
            self,
            func: Callable,
            *args: Any,
            _groupdata: bool = True,
            _drop_index: bool = True,
            _spike_groupdata: Optional[DataFrame] = None,
            **kwargs: Any
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
            self._group_data
            if _spike_groupdata is None
            else _spike_groupdata
        )
        def apply_func(gdata_row):
            index = gdata_row[0]
            subdf = self.iloc[gdata_row[1]['_rows'], :]
            if _drop_index:
                subdf.reset_index(drop=True, inplace=True)
            subdf.attrs['group_index'] = index
            subdf.attrs['group_data'] = groupdata
            ret = func(subdf, *args, **kwargs)
            if ret is None:
                return ret

            if _groupdata:
                # attaching grouping data
                gdata = groupdata.loc[
                    [index] * ret.shape[0],
                    [gvar for gvar in self._group_vars if gvar not in ret]
                ]
                gdata.index = ret.index
                ret = pandas.concat([gdata, ret], axis=1)
            return ret

        to_concat = [
            apply_func(row) for row in groupdata.iterrows()
        ]
        if all(elem is None for elem in to_concat):
            return None
        ret = pandas.concat(to_concat, axis=0)
        if _drop_index:
            ret.reset_index(drop=True, inplace=True)

        ret.attrs['groupby_drop'] = self.attrs['groupby_drop']
        return ret

    def __repr__(self) -> str:
        """Including grouping information when printed"""
        ret = super().__repr__()
        ngroups = self._group_data.shape[0]
        ret = f"{ret}\n[Groups: {self._group_vars} (n={ngroups})]"
        return ret

    def copy(self, deep: bool = True) -> "DataFrameGroupBy":
        """Copy the dataframe and keep the class"""
        return DataFrameGroupBy(
            super().copy() if deep else self,
            _group_vars=self._group_vars,
            _drop=self.attrs.get('groupby_drop', True),
            _group_data=self._group_data.copy() if deep else self._group_data
        )

    def _repr_html_(self) -> str:
        out = super()._repr_html_()
        ngroups = self._group_data.shape[0]
        return f"{out}[Groups: {self._group_vars} (n={ngroups})]"

class DataFrameRowwise(DataFrameGroupBy): # pylint: disable=too-many-ancestors
    """Group data frame rowwisely"""
    def _compute_group_data(self):
        _rows = Series(
            [[i] for i in range(self.shape[0])],
            name='_rows',
            dtype=object
        )
        if not self._group_vars:
            return _rows.to_frame()

        gdata = self[self._group_vars].copy()
        gdata['_rows'] = _rows
        return gdata

    def group_apply(
            self,
            func: Callable,
            *args: Any,
            _groupdata: bool = True,
            _drop_index: bool = True,
            _spike_groupdata: Optional[DataFrame] = None,
            **kwargs: Any
    ) -> DataFrame:
        if _spike_groupdata is not None:
            raise ValueError('`_spike_groupdata` not allowed.')
        if self._group_vars:
            gdata = self._group_data
        else:
            gdata = self.copy()
            gdata['_rows'] = self._group_data

        return super().group_apply(
            func,
            *args,
            _groupdata=_groupdata,
            _drop_index=_drop_index,
            _spike_groupdata=gdata,
            **kwargs
        )

    # pylint: disable=bad-super-call
    def __repr__(self) -> str:
        """Including grouping information when printed"""
        ret = super(DataFrameGroupBy, self).__repr__()
        ret = f"{ret}\n[Rowwise: {self._group_vars}]"
        return ret

    def copy(self, deep: bool = True) -> "DataFrameRowwise":
        return DataFrameRowwise(
            super(DataFrameGroupBy, self).copy() if deep else self,
            _group_vars=self._group_vars,
            _drop=self.attrs.get('groupby_drop', True),
            _group_data=self._group_data.copy() if deep else self._group_data
        )

    def _repr_html_(self) -> str:
        out = super()._repr_html_()
        ngroups = self._group_data.shape[0]
        return f"{out}[Rowwise: {self._group_vars} (n={ngroups})]"
