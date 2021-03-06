import builtins

from typing import Any, Iterable, List, Mapping, Optional, Set, Tuple, Union
from abc import ABC
from threading import Lock

from pandas import DataFrame
from pandas.core.series import Series
from pandas.core.groupby import DataFrameGroupBy
from pipda.symbolic import DirectRefAttr
from pipda.context import Context, ContextBase, ContextSelect
from pipda.utils import DataContext

from .utils import IterableLiterals, objectize, expand_collections, list_diff, sanitize_slice, select_columns
from .contexts import ContextSelectSlice

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
            data: Union[DataFrame, DataFrameGroupBy],
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
            if isinstance(elems, IterableLiterals):
                self.elems = Collection(*elems)
            else:
                self.elems = Collection(elems)
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

    def __init__(self, data, cols, fns, names, args, kwargs):
        cols = cols or objectize(data).columns
        if not isinstance(cols, (list, tuple)):
            cols = [cols]
        cols = select_columns(objectize(data).columns, *cols)

        fns_list = []
        if callable(fns):
            fns_list.append({'fn': fns})
        elif isinstance(fns, (list, tuple)):
            fns_list.extend(
                {'fn': fn, '_fn': i+1, '_fn0': i}
                for i, fn in enumerate(fns)
            )
        elif isinstance(fns, dict):
            fns_list.extend(
                {'fn': value, '_fn': key}
                for key, value in fns.items()
            )
        # else:
        # todo: check format of fns

        self.data = data
        self.cols = cols
        self.fns = fns_list
        self.names = names
        self.args = args
        self.kwargs = kwargs
        self.context = None

    def desc_cols(self) -> Set[str]:
        from ..dplyr.funcs import desc
        if len(self.fns) != 1:
            return set()
        if self.fns[0]['fn'] is not desc:
            return set()
        return set(self.cols)

    def evaluate(
            self,
            context: Union[Context, ContextBase],
            data: Optional[Union[DataFrame, DataFrameGroupBy]] = None
    ) -> Any:
        if data is None:
            data = self.data

        if not self.fns:
            return self.cols

        if isinstance(context, Context):
            context = context.value

        if isinstance(context, ContextSelect):
            fn = self.fns[0]['fn']
            # todo: check # fns
            pipda_type = getattr(fn, '__pipda__', None)
            return [
                fn(col, *self.args, **self.kwargs) if not pipda_type
                else fn(
                    col,
                    *CurColumn.replace_args(self.args, col),
                    **CurColumn.replace_kwargs(self.kwargs, col),
                    _calling_type='piping'
                ).evaluate(data)
                for col in self.cols
            ]

        ret = {}
        # Todo: groupby
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
                pipda_type = getattr(fn, '__pipda__', None)
                if not pipda_type:
                    ret[name] = fn(
                        context.getattr(data, column),
                        *CurColumn.replace_args(self.args, column),
                        **CurColumn.replace_kwargs(self.kwargs, column)
                    )
                else:
                    # use fn's own context
                    ret[name] = fn(
                        DirectRefAttr(data, column),
                        *CurColumn.replace_args(self.args, column),
                        **CurColumn.replace_kwargs(self.kwargs, column),
                        _calling_type='piping'
                    ).evaluate(data)
        return ret

class CAcross(Across):

    def __init__(self, data, cols, fns, names, args, kwargs):
        super().__init__(data, cols, fns, names, args, kwargs)

        if not self.fns:
            raise ValueError(
                "No functions specified for c_across. "
                "Note that the usage of c_across is different from R's. "
                "You have to specify the function inside c_across, instead of "
                "calling it with c_across(...) as arguments."
            )

        if len(self.fns) > 1:
            raise ValueError("Only a single function is allowed in c_across.")

        self.fn = self.fns[0]['fn']

    def evaluate(
            self,
            context: Union[Context, ContextBase],
            data: Optional[DataFrame] = None
    ) -> Any:
        if isinstance(context, Context):
            context = context.value

        if data is None:
            data = self.data

        if not isinstance(data, RowwiseDataFrame):
            return super().evaluate(context, data)

        return {
            self.names: data[self.cols].apply(
                self.fn,
                axis=1,
                args=self.args,
                **self.kwargs
            )
        }

class IfCross(Across, ABC):

    if_type = None

    def __init__(self, data, cols, fns, names, args, kwargs):
        super().__init__(data, cols, fns, names, args, kwargs)

        func_name = f"if_{self.__class__.if_type}"
        if not self.fns:
            raise ValueError(f"No functions specified for {func_name!r}.")

        if len(self.fns) > 1:
            raise ValueError(
                f"Only a single function is allowed in {func_name!r}."
            )

        self.fn = self.fns[0]['fn']

    def evaluate(
            self,
            context: Union[Context, ContextBase],
            data: Optional[DataFrame] = None
    ) -> Any:
        if not self.fns:
            raise ValueError("No functions specified for if_any.")

        if isinstance(context, Context):
            context = context.value

        if data is None:
            data = self.data

        agg_func = getattr(builtins, self.__class__.if_type)

        pipda_type = getattr(self.fn, '__pipda__', None)
        if pipda_type not in (None, 'PlainFunction'):
            def transform_fn(*args, **kwargs):
                return self.fn(data, *args, **kwargs)
            transform_fn = lambda *args, **kwargs: self.fn(
                data, *args, **kwargs
            )
        else:
            transform_fn = self.fn

        def if_fn(_series, *args, **kwargs):
            return agg_func(_series.transform(transform_fn, *args, **kwargs))

        return data[self.cols].apply(
            if_fn,
            axis=1,
            args=self.args,
            **self.kwargs
        )

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
        self.__dict__['rowwise'] = rowwise or True
        super().__init__(*args, **kwargs)

class ContextWithData:

    def __init__(self, data: Any) -> None:
        self.data = DataContext(data)

    def __enter__(self) -> Any:
        return self.data

    def __exit__(self, *exc_info) -> None:
        self.data.delete()
