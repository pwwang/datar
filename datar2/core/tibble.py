from itertools import chain
from typing import Callable, Mapping, Union, Sequence

from pandas._typing import Dtype
from pandas.api.types import is_array_like, is_scalar
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy
from pipda import evaluate_expr

from .collections import Collection
from .contexts import Context
from .utils import name_of, add_to_tibble
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
        if len(names) != len(data):
            raise ValueError(
                "Lengths of `names` and `values` are not the same."
            )
        names = repair_names(names, _name_repair)

        out = None
        for name, value in zip(names, data):
            value = evaluate_expr(value, out, Context.EVAL)
            if isinstance(value, Collection):
                value.expand()
            out = add_to_tibble(out, name, value, allow_dup_names=True)

        out = Tibble() if out is None else out
        if _dtypes in (None, False):
            return out
        if _dtypes is True:
            return out.convert_dtypes()

        if not isinstance(_dtypes, dict):
            dtypes = zip(out.columns, [_dtypes] * out.shape[1])
        else:
            dtypes = _dtypes.items()

        for column, dtype in dtypes.items():
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

            result = self.loc[:, subdf_cols]
            result.columns = [col[len(key) + 1 :] for col in subdf_cols]

        return result

    def __setitem__(self, key, value):
        if isinstance(value, (DataFrameGroupBy, SeriesGroupBy)):
            value = value.obj

        if isinstance(value, DataFrame):
            value = value.copy()
            key = value.columns = [f"{key}${col}" for col in value.columns]
            # recycle shape (1, x) data frames
            if (
                self.shape[0] > value.shape[0]
                and value.shape[0] == 1
                and value.index[0] == 0
            ):
                value = value.reindex([0] * self.shape[0])
                value.index = self.index

        # broadcast myself to the coming data if I am constructed by a scalar
        if (
            self.shape[0] == 1
            and self.index[0] == 0
            and (isinstance(value, Sequence) or is_array_like(value))
            and len(value) > 1
        ):
            df = self.reindex([0] * len(value))
            self.__class__.__init__(self, df, meta=self._datar_meta)
            if isinstance(value, (Series, DataFrame)):
                self.index = value.index
            else:  # array/list, use range index
                self.index = range(len(value))

        # also allow lenth-1 sequences to work like scalar
        elif (
            (isinstance(value, Sequence) or is_array_like(value))
            and not isinstance(value, (Series, DataFrame))
            and len(value) == 1
            and is_scalar(value[0])
        ):
            value = value[0]

        return super().__setitem__(key, value)

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
        grouped = self.groupby(cols, observed=drop, sort=sort, dropna=dropna)
        meta = {"grouped": grouped}
        return TibbleGroupby(self, meta=meta)


class TibbleGroupby(Tibble):
    """"""

    @property
    def _constructor(self):
        return TibbleGroupby

    def get_obj(self, key=None):
        if key is None:
            return self._datar_meta["grouped"].obj

        return self._datar_meta["grouped"].obj[key]

    @classmethod
    def from_grouped(
        cls,
        grouped: Union[SeriesGroupBy, DataFrameGroupBy],
    ) -> "TibbleGroupby":
        """"""
        if isinstance(grouped, SeriesGroupBy):
            grouped = grouped.obj.to_frame().groupby(
                grouped.grouper,
                observed=grouped.observed,
                sort=grouped.sort,
                dropna=grouped.dropna,
            )

        return TibbleGroupby(grouped.obj, meta={"grouped": grouped})

    def __getitem__(self, key):
        result = super().__getitem__(key)
        if isinstance(result, Series):
            return self._datar_meta["grouped"][key]

        return result


class TibbleRowwise(TibbleGroupby):
    """"""

    @property
    def _constructor(self):
        return TibbleRowwise
