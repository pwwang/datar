from typing import Any, Iterable, Optional, Union

from pandas import DataFrame
from pandas.core.series import Series
from pandas.core.groupby import DataFrameGroupBy
from pipda.symbolic import DirectRefAttr
from pipda.context import Context, ContextBase, ContextSelect

from .utils import arithmetize, expand_collections, list_diff, sanitize_slice, select_columns
from .group_by import get_rowwise, is_grouped, get_groups


class Collection(list):
    """Mimic the c function in R

    All elements will be flattened

    Args:
        *args: The elements
    """
    def __init__(self, *args: Any) -> None:
        super().__init__(expand_collections(args))

class Inverted:
    """Inverted object, pending for next action"""

    def __init__(self, elems: Any, data: DataFrame) -> None:
        self.data = data
        if isinstance(elems, slice):
            columns = data.columns.tolist()
            self.elems = columns[sanitize_slice(elems, columns)]
        elif not isinstance(elems, Collection):
            self.elems = Collection(elems)
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
        if self._complements is None:
            self._complements = list_diff(self.data.columns, self.elems)
        return self._complements

    def __repr__(self) -> str:
        return f"Inverted({self.elems})"

class DescSeries(Series):

    @property
    def _constructor(self):
        return DescSeries

class Across:

    def __init__(self, data, cols, fns, names, args, kwargs):
        cols = cols or arithmetize(data).columns
        if not isinstance(cols, (list, tuple)):
            cols = [cols]
        cols = select_columns(arithmetize(data).columns, *cols)

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
                    *self.args,
                    **self.kwargs,
                    _force_piping=True
                ).evaluate(data)
                for col in self.cols
            ]

        ret = {}
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
                        *self.args,
                        **self.kwargs,
                    )
                else:
                    # use fn's own context
                    ret[name] = fn(
                        DirectRefAttr(data, column),
                        *self.args,
                        **self.kwargs,
                        _force_piping=True
                    ).evaluate(data)

        return ret

class CAcross(Across):

    def evaluate(self, context: Context, data: Optional[DataFrame] = None):
        if not self.fns:
            raise ValueError(
                "No functions specified for c_across. "
                "Note that the usage of c_across is different from R's. "
                "You have to specify the function inside c_across, instead of "
                "calling it with c_across(...) as arguments."
            )

        if data is None:
            data = self.data

        if not get_rowwise(data):
            return super().evaluate(context, data)

        # todo: check, only one function and one name is allowed
        return {
            self.names: data[self.cols].apply(
                self.fns[0]['fn'],
                axis=1,
                args=self.args,
                **self.kwargs
            ).values
        }

class RowwiseDataFrame(DataFrame):

    def __init__(
            self,
            *args: Any,
            rowwise: Optional[Iterable[str]] = None,
            **kwargs: Any
    ) -> None:
        self.__dict__['rowwise'] = rowwise or True
        super().__init__(*args, **kwargs)
