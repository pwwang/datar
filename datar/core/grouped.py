"""Provide DataFrameGroupBy class"""
from typing import Any, Callable, List, Optional
from itertools import product
from collections import defaultdict

import numpy
import pandas
from pandas.core.arrays.categorical import Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from pandas import DataFrame, Series

from ..base.constants import NA

class DataFrameGroupBy(DataFrame): # pylint: disable=too-many-ancestors
    """GroupBy data frame, instead of the one from pandas

    Because:
    1. Pandas DataFrameGroupBy cannot handle mutilpe categorical columns as
        groupby variables
    2. It is very hard to retrieve group indices and data when doing apply
    """
    def __init__(
            self,
            *args: Any,
            _group_vars: Optional[List[str]] = None,
            _drop: Optional[bool] = None,
            _group_data: Optional[DataFrame] = None,
            **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.reset_index(drop=True, inplace=True)

        if _drop is None:
            _drop = self.attrs.get('groupby_drop', True)
        self.attrs['groupby_drop'] = _drop

        self.__dict__['_group_vars'] = _group_vars or []
        if not _group_vars:
            self.__dict__['_group_data'] = DataFrame({
                '_rows': [list(range(self.shape[0]))]
            })
        elif _group_data is None:
            self.__dict__['_group_data'] = self._compute_group_data()
        else:
            self.__dict__['_group_data'] = _group_data

    def _compute_group_data(self):
        """Compute group data"""
        # group by values have to be hashable
        gdata = self[self._group_vars].sort_values(self._group_vars)
        if self.attrs['groupby_drop']:
            rows_dict = defaultdict(lambda: [])
            for row in gdata.itertuples():
                rows_dict[row[1:]].append(row.Index)
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

                vardata = vardata[~pandas.isna(vardata)]

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
            **kwargs: Any
    ) -> DataFrame:
        """Apply function to each group

        Args:
            func: The function applied to the groups
            *args, **kwargs: The arguments for func
            _groupdata: Whether to include the group data in the results or not
        """

        def apply_func(gdata_row):
            index = gdata_row[0]
            subdf = self.iloc[gdata_row[1]['_rows'], :]
            subdf.reset_index(drop=True, inplace=True)
            subdf.attrs['group_index'] = index
            subdf.attrs['group_data'] = self._group_data
            ret = func(subdf, *args, **kwargs)
            if ret is None:
                return ret
            ret.reset_index(drop=True, inplace=True)

            if _groupdata:
                # attaching grouping data
                gdata = self._group_data.loc[
                    [index] * ret.shape[0],
                    [gvar for gvar in self._group_vars if gvar not in ret]
                ].reset_index(drop=True)
                ret = pandas.concat([gdata, ret], axis=1)
            return ret

        to_concat = [
            apply_func(row) for row in self._group_data.iterrows()
        ]
        if all(elem is None for elem in to_concat):
            return None
        ret = pandas.concat(to_concat, axis=0).reset_index(drop=True)

        ret.attrs['groupby_drop'] = self.attrs['groupby_drop']
        return ret

    def __repr__(self) -> str:
        """Including grouping information when printed"""
        ret = super().__repr__()
        ngroups = self._group_data.shape[0]
        ret = f"{ret}\n[Groups: {self._group_vars} (n={ngroups})]"
        return ret

    def copy(self, deep: bool = True) -> "DataFrameGroupBy":
        return DataFrameGroupBy(
            super().copy() if deep else self,
            _group_vars=self._group_vars,
            _drop=self.attrs.get('groupby_drop', True),
            _group_data=self._group_data.copy() if deep else self._group_data
        )
