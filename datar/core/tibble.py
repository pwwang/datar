from itertools import chain
from typing import TYPE_CHECKING, Callable, Mapping, Union, Sequence

import numpy as np
from pipda import evaluate_expr

from .backends.pandas import DataFrame, Index, Series
from .backends.pandas.api.types import is_scalar
from .backends.pandas.core.groupby import SeriesGroupBy, GroupBy

from .utils import apply_dtypes, name_of
from .names import repair_names

if TYPE_CHECKING:
    from pandas._typing import Dtype


class Tibble(DataFrame):
    """Tibble class - A pandas.DataFrame subclass

    A metadata is added: `_datar`, used to save some information, including
    grouped and grouping vars

    Args:
        meta: The metadata
    """

    _metadata = ["_datar"]

    def __init__(self, data=None, *args, meta=None, **kwargs):
        """Construct a tibble"""
        super().__init__(data, *args, **kwargs)
        self._datar = meta or {}

    @property
    def _constructor(self):
        return Tibble

    @classmethod
    def from_pairs(
        cls,
        names: Sequence[str],
        data: Sequence,
        _name_repair: Union[str, Callable] = "check_unique",
        _dtypes: Union["Dtype", Mapping[str, "Dtype"]] = None,
    ) -> "Tibble":
        """Construct a tibble with name-value pairs

        Instead of do `**kwargs`, this allows duplicated names

        Args:
            names: The names of the data to be construct a tibble
            data: The data to construct a tibble, must have the same length
                with the names
            _name_repair: How to repair names
            _dtypes: The dtypes for post conversion
        """
        from .broadcast import add_to_tibble
        from .collections import Collection
        from .contexts import Context

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

            out = add_to_tibble(
                out,
                name,
                value,
                allow_dup_names=True,
                broadcast_tbl=True,
            )

        out = Tibble() if out is None else out

        if _dtypes in (None, False):
            return out
        if _dtypes is True:
            return out.convert_dtypes()

        apply_dtypes(out, _dtypes)
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

        Args:
            *args: and
            **kwargs: The data used to construct the tibble
            _name_repair: How to repair names
            _rows: When `*args` and `**kwargs` are not given, construct an
                empty tibble with `_rows` rows.
            _dtypes: The dtypes for post conversion
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
                col for col in self.columns if str(col).startswith(f"{key}$")
            ]
            if not subdf_cols:
                raise

            result = Tibble(self.loc[:, subdf_cols])
            result.columns = [col[len(key) + 1 :] for col in subdf_cols]

        return result

    def __setitem__(self, key, value):
        from .broadcast import broadcast_to

        value = broadcast_to(value, self.index)

        # if isinstance(value, GroupBy):
        #     value = value.obj

        if isinstance(key, str) and isinstance(value, DataFrame):
            if value.shape[1] == 0:
                super().__setitem__(key, np.nan)

            else:
                for col in value.columns:
                    colname = f"{key}${col}"
                    super().__setitem__(colname, value[col].copy())

        else:
            super().__setitem__(key, value)

    def group_by(
        self,
        cols,
        add=False,
        drop=None,
        sort=False,
        dropna=False,
    ) -> "TibbleGrouped":
        """Group a tibble by variants"""
        if drop is None:
            drop = False

        cols = [cols] if is_scalar(cols) else list(cols)
        grouped = self.groupby(cols, observed=drop, sort=sort, dropna=dropna)

        return TibbleGrouped.from_groupby(grouped)

    def rowwise(self, cols: Sequence[str] = None) -> "TibbleRowwise":
        """Get a rowwise tibble"""
        cols = (
            []
            if cols is None
            else [cols]
            if isinstance(cols, str)
            else list(cols)
        )
        grouped = self.groupby(
            Index(range(self.shape[0])),
            sort=False,
            observed=True,
            dropna=False,
        )
        return TibbleRowwise(
            self,
            meta={"group_vars": cols, "grouped": grouped},
        )

    @property
    def group_vars(self) -> Sequence[str]:
        """Get the grouping variables."""
        return []


class TibbleGrouped(Tibble):
    """Grouped tibble.

    The `DataFrameGroupBy` object is hold at `df._datar["grouped"]`
    """

    def __init__(self, data=None, *args, meta=None, **kwargs):
        super().__init__(data, *args, meta=meta, **kwargs)
        # meta could be copied by operations since it's _metadata
        if "grouped" in self._datar:
            # make a shallow copy of the df to obj
            # so that the changes can reflect to obj
            self.regroup(hard=False)

    @property
    def _constructor(self):
        return TibbleGrouped

    @property
    def _html_footer(self):
        groups = ", ".join((str(name) for name in self.group_vars))
        return (
            f"<p>{self.__class__.__name__}: {groups} "
            f"(n={self._datar['grouped'].grouper.ngroups})"
        )

    @property
    def _str_footer(self):
        groups = ", ".join((str(name) for name in self.group_vars))
        return (
            f"[{self.__class__.__name__}: {groups} "
            f"(n={self._datar['grouped'].grouper.ngroups})]"
        )

    @classmethod
    def from_groupby(
        cls,
        grouped: GroupBy,
        name: str = None,
        deep: bool = True,
    ) -> "TibbleGrouped":
        """"""
        if isinstance(grouped, SeriesGroupBy):
            frame = (
                grouped.obj.to_frame(name) if name else grouped.obj.to_frame()
            )
            grouped = frame.groupby(
                grouped.grouper,
                observed=grouped.observed,
                sort=grouped.sort,
                dropna=grouped.dropna,
            )

        meta = {"grouped": grouped}
        if cls is TibbleRowwise:
            meta["group_vars"] = []

        return cls(grouped.obj, copy=deep, meta=meta)

    def __getitem__(self, key):
        result = super().__getitem__(key)
        grouped = self._datar["grouped"]
        if isinstance(result, Series):
            return grouped[key]
        if isinstance(result, DataFrame):
            newmeta = self._datar.copy()
            newmeta["grouped"] = result.groupby(
                grouped.grouper,
                sort=grouped.sort,
                observed=grouped.observed,
                dropna=grouped.dropna,
            )
            out = TibbleGrouped(result, copy=False, meta=newmeta)
            return out
        return result  # pragma: no cover

    def __setitem__(self, key, value):
        from .broadcast import broadcast_to

        grouped = self._datar["grouped"]
        value = broadcast_to(value, self.index, grouped.grouper)

        if isinstance(key, str) and isinstance(value, DataFrame):
            for col in value.columns:
                colname = f"{key}${col}"
                DataFrame.__setitem__(self, colname, value[col])
        else:
            DataFrame.__setitem__(self, key, value)

    def regroup(self, hard=True, inplace=True) -> "TibbleGrouped":
        """Apply my grouping settings to another data frame"""
        # hard = False, structure and grouping variables not changed
        if not hard:
            if inplace:
                self._datar["grouped"].obj = Tibble(self, copy=False)
                return self

            out = self.copy()
            out._datar["grouped"].obj = Tibble(out, copy=False)
            return out

        grouped = self._datar["grouped"]
        new = Tibble(self, copy=False).group_by(
            self.group_vars,
            drop=grouped.observed,
            sort=grouped.sort,
            dropna=grouped.dropna,
        )
        if not inplace:
            return new

        self._datar = new._datar
        self._datar["grouped"].obj = Tibble(self, copy=False)
        return self

    def transform(self, *args, **kwargs):
        """Transform brings the metadata of original df, we need to update it"""
        out = super().transform(*args, **kwargs)
        # pandas < 1.4, _datar not carried by transform
        return reconstruct_tibble(self, out)

    def copy(self, deep: bool = True) -> "TibbleGrouped":
        grouped = self._datar["grouped"]
        return self.__class__.from_groupby(
            grouped.obj.groupby(
                grouped.grouper,
                observed=grouped.observed,
                sort=grouped.sort,
                dropna=grouped.dropna,
            ),
            deep=deep,
        )

    def reindex(self, *args, **kwargs) -> "TibbleGrouped":
        result = Tibble.reindex(self, *args, **kwargs)
        result.reset_index(drop=True, inplace=True)
        return result.regroup(inplace=False)

    def take(self, *args, **kwargs) -> "TibbleGrouped":
        result = Tibble.take(self, *args, **kwargs)
        if kwargs.get("axis", 0) == 1:
            return result

        result.reset_index(drop=True, inplace=True)
        return result.regroup(inplace=False)

    def sample(self, *args, **kwargs) -> "TibbleGrouped":
        grouped = self._datar["grouped"]
        result = grouped.sample(*args, **kwargs)
        result.reset_index(drop=True, inplace=True)
        return reconstruct_tibble(self, result)

    def convert_dtypes(self, *args, **kwargs) -> "TibbleGrouped":
        out = DataFrame.convert_dtypes(self, *args, **kwargs)
        return reconstruct_tibble(self, out)

    def group_by(
        self,
        cols,
        add=False,
        drop=None,
        sort=False,
        dropna=False,
    ) -> "TibbleGrouped":
        """Group a tibble by variants"""
        if add and "grouped" in self._datar:
            from ..base import union

            cols = list(union(self.group_vars, cols, __ast_fallback="normal"))

        return Tibble.group_by(
            Tibble(self, copy=False),
            cols,
            drop=drop,
            sort=sort,
            dropna=dropna,
        )

    @property
    def group_vars(self) -> Sequence[str]:
        # When column names changed, we save the new group vars
        return self._datar.get(
            "group_vars", self._datar["grouped"].grouper.names
        )


class TibbleRowwise(TibbleGrouped):
    """Rowwise tibble

    Examples:
        >>> df = Tibble({"a": [1, 2, 3]}).rowwise([])
        >>> df.a  # SeriesGroupBy
        >>> df.a.is_rowwise  # True
    """

    @property
    def _constructor(self):
        return TibbleRowwise

    @property
    def group_vars(self) -> Sequence[str]:
        """Get the grouping variables"""
        return self._datar["group_vars"]

    def __getitem__(self, key):
        result = super().__getitem__(key)
        if isinstance(result, SeriesGroupBy):
            result.is_rowwise = True
        elif isinstance(result, DataFrame):
            return reconstruct_tibble(self, result)
        return result

    def copy(self, deep: bool = True) -> "TibbleRowwise":
        out = super().copy(deep=deep)
        out._datar["group_vars"] = self._datar["group_vars"]
        return out

    def regroup(self, hard=True, inplace=True) -> "TibbleRowwise":
        """Apply my grouping settings to another data frame"""
        # hard = False, structure and grouping variables not changed
        if not hard:
            out = super().regroup(hard=hard, inplace=inplace)
            out._datar["group_vars"] = self.group_vars
            return out

        new = self.groupby(
            Index(range(self.shape[0])),
            observed=True,
            dropna=False,
            sort=False,
        )
        if not inplace:
            out = self.__class__.from_groupby(new)
            out._datar["group_vars"] = self.group_vars
            return out

        self._datar["grouped"] = new
        self._datar["grouped"].obj = Tibble(self, copy=False)
        self._datar["group_vars"] = self.group_vars
        return self


class SeriesRowwise(SeriesGroupBy):
    """Class only used for dispatching"""


class SeriesCategorical(Series):
    """Class only used for dispatching"""


def reconstruct_tibble(orig, out, drop=None, ungrouping_vars=None):
    """Try to reconstruct the structure of `out` based on `orig`

    The rule is
    1. if `orig` is a rowwise df, `out` will also be a rowwise df
        grouping vars are the ones from `orig` if exist, otherwise empty
    2. if `orig` is a grouped df, `out` will only be a grouped df when
        any grouping vars of `orig` can be found in out. If none of the
        grouping vars can be found, just return a plain df
    3. If `orig` is a plain df, `out` is also a plain df

    For TibbleGrouped object, `_drop` attribute is maintained.

    Args:
        orig: The original df
        out: The target df
        drop: Drop empty groups, only for grouped df
            If None, will use orig's drop

    Returns:
        The reconstructed out
    """
    from ..base import intersect, setdiff

    if not isinstance(out, Tibble):
        out = Tibble(out, copy=False)

    if isinstance(orig, TibbleRowwise):
        gvars = intersect(orig.group_vars, out.columns, __ast_fallback="normal")
        gvars = setdiff(gvars, ungrouping_vars or [], __ast_fallback="normal")
        out = out.rowwise(gvars)

    elif isinstance(orig, TibbleGrouped):
        gvars = intersect(orig.group_vars, out.columns, __ast_fallback="normal")
        gvars = setdiff(gvars, ungrouping_vars or [], __ast_fallback="normal")
        if len(gvars) > 0:
            out = out.group_by(
                gvars,
                drop=orig._datar["grouped"].observed if drop is None else drop,
                sort=orig._datar["grouped"].sort,
                dropna=orig._datar["grouped"].dropna,
            )

    return out
