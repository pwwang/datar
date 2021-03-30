import builtins

from typing import (
    Any, Callable, Iterable, List, Mapping, Optional, Set, Tuple, Union
)
from abc import ABC
from threading import Lock

from pandas import DataFrame
from pandas.core.series import Series
from pipda.symbolic import DirectRefAttr
from pipda.context import Context, ContextBase, ContextSelect
from pipda.utils import DataEnv, Expression, functype

from .utils import (
    align_value, df_assign_item, objectize, expand_collections, list_diff, sanitize_slice, select_columns,
    logger, to_df
)
from .contexts import ContextSelectSlice
from .types import DataFrameType, is_scalar

LOCK = Lock()

class Collection(list):
    """Mimic the c function in R

    All elements will be flattened

    Args:
        *args: The elements
    """
    def __init__(self, *args: Any) -> None:
        super().__init__(expand_collections(args))

    def expand_slice(
            self,
            total: Union[int, Iterable[int]]
    ) -> Union[List[int], List[List[int]]]:
        """Expand the slice in the list in a groupby-aware way"""


class Inverted:
    """Inverted object, pending for next action"""

    def __init__(
            self,
            elems: Any,
            data: DataFrameType,
            context: ContextBase = Context.SELECT.value
    ) -> None:
        self.data = objectize(data)
        self.context = context
        if isinstance(elems, slice):
            if isinstance(context, ContextSelectSlice):
                self.elems = [elems]
            else:
                columns = self.data.columns.tolist()
                self.elems = columns[sanitize_slice(elems, columns)]
        elif not isinstance(elems, Collection):
            if is_scalar(elems):
                self.elems = Collection(elems)
            else:
                self.elems = Collection(*elems)
        elif not isinstance(context, ContextSelectSlice):
            columns = self.data.columns.to_list()
            self.elems = [
                columns[elem] if isinstance(elem, int) else elem
                for elem in elems
            ]
        else:
            self.elems = elems
        self._complements = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Inverted):
            return False
        return self.elem == other.elem and self.data == other.data

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    @property
    def complements(self):
        if isinstance(self.context, ContextSelectSlice):
            # slice literal not being expanded
            return self
        if self._complements is None:
            self._complements = list_diff(self.data.columns, self.elems)
        return self._complements

    def __repr__(self) -> str:
        return f"Inverted({self.elems})"

class Negated:

    def __init__(self, elems: Union[slice, list]) -> None:
        """In case of -[1,2,3] or -c(1,2,3) or -f[1:3]"""
        self.elems = [elems] if isinstance(elems, slice) else elems

    def __repr__(self) -> str:
        return f"Negated({self.elems})"

class DescSeries(Series):

    @property
    def _constructor(self):
        return DescSeries

class CurColumn:

    @classmethod
    def replace_args(cls, args: Tuple[Any], column: str) -> Tuple[Any]:
        return tuple(column if isinstance(arg, cls) else arg for arg in args)

    @classmethod
    def replace_kwargs(
            cls,
            kwargs: Mapping[str, Any],
            column: str
    ) -> Mapping[str, Any]:
        return {
            key: column if isinstance(val, cls) else val
            for key, val in kwargs.items()
        }

class Across:

    def __init__(
            self,
            data: DataFrameType,
            cols: Optional[Iterable[str]] = None,
            fns: Optional[Union[
                Callable,
                Iterable[Callable],
                Mapping[str, Callable]
            ]] = None,
            names: Optional[str] = None,
            kwargs: Optional[Mapping[str, Any]] = None
    ) -> None:
        from ..dplyr.funcs import everything
        cols = everything(data) if cols is None else cols
        if not isinstance(cols, (list, tuple)):
            cols = [cols]
        cols = select_columns(objectize(data).columns, *cols)

        fns_list = []
        if callable(fns):
            fns_list.append({'fn': fns})
        elif isinstance(fns, (list, tuple)):
            fns_list.extend(
                {'fn': fn, '_fn': i, '_fn1': i+1}
                for i, fn in enumerate(fns)
            )
        elif isinstance(fns, dict):
            fns_list.extend(
                {'fn': value, '_fn': key}
                for key, value in fns.items()
            )
        elif fns is not None:
            raise ValueError(
                'Argument `_fns` of across must be None, a function, '
                'a formula, or a dict of functions.'
            )

        self.data = data
        self.cols = cols
        self.fns = fns_list
        self.names = names
        self.kwargs = kwargs or {}

    def evaluate(self, context: Optional[ContextBase] = None) -> DataFrame:

        if not self.fns:
            self.fns = [{'fn': lambda x: x}]

        ret = None
        for column in self.cols:
            for fn_info in self.fns:
                render_data = fn_info.copy()
                render_data['_col'] = column
                fn = render_data.pop('fn')
                name_format = self.names
                if not name_format:
                    name_format = (
                        '{_col}_{_fn}' if '_fn' in render_data
                        else '{_col}'
                    )

                name = name_format.format(**render_data)
                if functype(fn) == 'plain':
                    value = fn(
                        self.data[column],
                        **CurColumn.replace_kwargs(self.kwargs, column)
                    )
                else:
                    # use fn's own context
                    value = fn(
                        DirectRefAttr(self.data, column),
                        **CurColumn.replace_kwargs(self.kwargs, column),
                        _env='piping'
                    )(self.data, context)

                # todo: check if it is proper
                #       group information lost
                value = objectize(value)
                if ret is None:
                    ret = to_df(value, name)
                else:
                    df_assign_item(ret, name, value)
        return DataFrame() if ret is None else ret

class IfCross(Across, ABC):

    if_type = None

    def evaluate(self, context: Optional[ContextBase] = None) -> DataFrame:

        agg_func = getattr(builtins, self.__class__.if_type)
        return super().evaluate(context).fillna(
            False
        ).astype(
            'boolean'
        ).apply(agg_func, axis=1)

class IfAny(IfCross):

    if_type = 'any'

class IfAll(IfCross):

    if_type = 'all'

class RowwiseDataFrame(DataFrame):

    def __init__(
            self,
            *args: Any,
            rowwise: Optional[Iterable[str]] = None,
            **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.flags.rowwise = rowwise or True

class WithDataEnv:

    def __init__(self, data: Any) -> None:
        self.data = DataEnv(data)

    def __enter__(self) -> Any:
        return self.data

    def __exit__(self, *exc_info) -> None:
        self.data.delete()

class Nesting:

    def __init__(self, *columns: Any, **kwargs: Any) -> None:
        self.columns = []
        self.names = []

        id_prefix = hex(id(self))[2:6]
        for i, column in enumerate(columns):
            self.columns.append(column)
            if isinstance(column, str):
                self.names.append(column)
                continue
            try:
                # series
                name = column.name
            except AttributeError:
                name = f'_tmp{id_prefix}_{i}'
                logger.warning(
                    'Temporary name used for a nesting column, use '
                    'keyword argument instead to specify the key as name.'
                )
            self.names.append(name)

        for key, val in kwargs.items():
            self.columns.append(val)
            self.names.append(key)

    def __len__(self):
        return len(self.columns)
