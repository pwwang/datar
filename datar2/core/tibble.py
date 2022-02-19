from itertools import chain
from typing import Callable, Mapping, Union, Sequence

from pandas._typing import Dtype
from pandas.api.types import is_scalar
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
from pandas.core.series import Series
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy
from pipda import evaluate_expr

from .collections import Collection
from .contexts import Context
from .utils import name_of, regcall
from .names import repair_names


class Tibble(DataFrame):
    """Tibble class"""

    _metadata = ["_datar_meta"]

    def __init__(self, data=None, *args, meta=None, **kwargs):
        super().__init__(data, *args, **kwargs)
        self._datar_meta = meta or {}

    @property
    def _constructor(self):
        return Tibble

    @classmethod
    def from_pairs(
        cls,
        names: Sequence[str],
        data: Sequence,
        _name_repair: Union[str, Callable] = "check_unique",
        _dtypes: Union[Dtype, Mapping[str, Dtype]] = None,
    ) -> "Tibble":
        from .broadcast import add_to_tibble
        from ..dplyr import ungroup

        if len(names) != len(data):
            raise ValueError(
                "Lengths of `names` and `values` are not the same."
            )
        names = repair_names(names, _name_repair)

        out = None
        for name, value in zip(names, data):
            value = evaluate_expr(value, out, Context.EVAL)
            value = regcall(ungroup, value)
            if isinstance(value, Collection):
                value.expand()
            out = add_to_tibble(
                out,
                name,
                value,
                allow_dup_names=True,
                broadcast_tbl=True,
            )

        out = Tibble() if out is None else out
        out.reset_index(drop=True, inplace=True)

        if _dtypes in (None, False):
            return out
        if _dtypes is True:
            return out.convert_dtypes()

        if not isinstance(_dtypes, dict):
            dtypes = zip(out.columns, [_dtypes] * out.shape[1])
        else:
            dtypes = _dtypes.items()

        for column, dtype in dtypes:
            if column in out:
                out[column] = out[column].astype(dtype)
            else:
                for col in out:
                    if col.startswith(f"{column}$"):
                        out[col] = out[col].astype(dtype)

        return out

    @classmethod
    def from_args(
        cls,
        *args,
        _name_repair="check_unique",
        _rows=None,
        _dtypes=None,
        **kwargs,
    ) -> "Tibble":
        """Construct tibble by given data, more like the tibble constructor in R
        """
        if not args and not kwargs:
            return cls() if not _rows else cls(index=range(_rows))

        names = [name_of(arg) for arg in args] + list(kwargs)
        return cls.from_pairs(
            names,
            list(chain(args, kwargs.values())),
            _name_repair=_name_repair,
            _dtypes=_dtypes,
        )

    def __getitem__(self, key):
        try:
            result = super().__getitem__(key)
        except KeyError:
            subdf_cols = [
                col for col in self.columns if col.startswith(f"{key}$")
            ]
            if not subdf_cols:
                raise

            result = Tibble(self.loc[:, subdf_cols])
            result.columns = [col[len(key) + 1 :] for col in subdf_cols]

        return result

    def __setitem__(self, key, value):
        from .broadcast import broadcast_to
        if isinstance(value, (DataFrameGroupBy, SeriesGroupBy)):
            value = value.obj

        value = broadcast_to(value, self.index)

        if isinstance(key, str) and isinstance(value, DataFrame):
            for col in value.columns:
                colname = f"{key}${col}"
                super().__setitem__(colname, value[col])

        else:
            super().__setitem__(key, value)

    def group_by(
        self,
        cols,
        add=False,
        drop=None,
        sort=False,
        dropna=False,
    ) -> "TibbleGroupby":
        """Group a tibble by variants"""
        if drop is None:
            drop = False

        cols = [cols] if is_scalar(cols) else list(cols)
        grouped = self.groupby(cols, observed=drop, sort=sort, dropna=dropna)

        return TibbleGroupby.from_grouped(grouped)

    def rowwise(self, cols) -> "TibbleRowwise":
        grouped = self.groupby(Index(range(self.shape[0])), sort=False)
        return TibbleRowwise(
            self,
            meta={"group_vars": cols, "grouped": grouped},
        )

    @property
    def group_vars(self) -> Sequence[str]:
        return []


class TibbleGroupby(Tibble):
    """"""

    def __init__(self, data=None, *args, meta=None, **kwargs):
        super().__init__(data, *args, meta=meta, **kwargs)
        # meta could be copied by operations since it's _metadata
        if "grouped" in self._datar_meta:
            # make a shallow copy of the df to obj
            # so that the changes can reflect to obj
            self._datar_meta["grouped"].obj = Tibble(self, copy=False)

    @property
    def _constructor(self):
        return TibbleGroupby

    @classmethod
    def from_grouped(
        cls,
        grouped: Union[SeriesGroupBy, DataFrameGroupBy],
        name: str = None,
        deep: bool = True,
    ) -> "TibbleGroupby":
        """"""
        if isinstance(grouped, SeriesGroupBy):
            frame = (
                grouped.obj.to_frame(name)
                if name
                else grouped.obj.to_frame()
            )
            grouped = frame.groupby(
                grouped.grouper,
                observed=grouped.observed,
                sort=grouped.sort,
                dropna=grouped.dropna,
            )

        return cls(grouped.obj, copy=deep, meta={"grouped": grouped})

    def __getitem__(self, key):
        result = super().__getitem__(key)
        if isinstance(result, Series):
            return self._datar_meta["grouped"][key]

        return result

    def __setitem__(self, key, value):
        from .broadcast import broadcast_to
        grouped = self._datar_meta["grouped"]
        value = broadcast_to(value, self.index, grouped.grouper)

        if isinstance(key, str) and isinstance(value, DataFrame):
            for col in value.columns:
                colname = f"{key}${col}"
                DataFrame.__setitem__(self, colname, value[col])
        else:
            DataFrame.__setitem__(self, key, value)

    def copy(self, deep: bool = True) -> "TibbleGroupby":
        grouped = self._datar_meta["grouped"]
        return self.__class__.from_grouped(
            grouped.obj.groupby(
                grouped.grouper,
                observed=grouped.observed,
                sort=grouped.sort,
                dropna=grouped.dropna,
            ),
            deep=deep,
        )

    def reindex(self, *args, **kwargs) -> "TibbleGroupby":
        result = Tibble(super().reindex(*args, **kwargs), copy=False)
        grouped = self._datar_meta["grouped"]
        return result.group_by(
            grouped.grouper.names,
            drop=grouped.observed,
            sort=grouped.sort,
            dropna=grouped.dropna,
        )

    def group_by(
        self,
        cols,
        add=False,
        drop=None,
        sort=False,
        dropna=False,
    ) -> "TibbleGroupby":
        """Group a tibble by variants"""
        if add and "grouped" in self._datar_meta:
            from ..base import union

            cols = regcall(union, self.group_vars, cols)

        grouped = self._datar_meta["grouped"].obj.groupby(
            cols, observed=drop, sort=sort, dropna=dropna
        )

        return TibbleGroupby.from_grouped(grouped)

    @property
    def group_vars(self) -> Sequence[str]:
        # When column names changed, we save the new group vars
        return self._datar_meta.get(
            "group_vars",
            self._datar_meta["grouped"].grouper.names
        )


class TibbleRowwise(TibbleGroupby):
    """"""

    @property
    def _constructor(self):
        return TibbleRowwise

    @property
    def group_vars(self) -> Sequence[str]:
        return self._datar_meta["group_vars"]

    def __getitem__(self, key):
        result = super().__getitem__(key)
        if isinstance(result, SeriesGroupBy):
            result.is_rowwise = True

        return result

    def reindex(self, *args, **kwargs) -> "TibbleGroupby":
        result = Tibble(Tibble.reindex(self, *args, **kwargs), copy=False)
        return result.rowwise(self.group_vars)
