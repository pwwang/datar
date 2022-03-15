"""Basic functions"""
from pandas import Series
from pandas.core.groupby import SeriesGroupBy
from pipda import evaluate_expr, register_func

from ..core.factory import func_factory
from ..core.contexts import Context
from ..core.collections import Collection
from ..core.utils import regcall


@func_factory("apply", "x")
def itemgetter(x, subscr, __args_raw=None):
    """Itemgetter as a function for verb

    In datar expression, we can do:
    >>> arr = [1,2,3]
    >>> tibble(x=2) >> mutate(y=arr[f.x])

    Since `arr[f.x]` won't compile. We need to use the `itemgetter` operator:
    >>> tibble(x=2) >> mutate(y=itemgetter(arr, f.x))

    Args:
        data: The data to be get items from
        subscr: The subscripts
    """
    # allow f[:2] to work
    subscr = evaluate_expr(subscr, x, Context.EVAL)
    if isinstance(subscr, Collection):
        subscr.expand(pool=x.size)

    if isinstance(subscr, Series):
        subscr = subscr.values

    out = x.iloc[subscr]
    if isinstance(__args_raw["x"], Series):
        return out
    return out.values


itemgetter.register(
    SeriesGroupBy,
    func=None,
    post=lambda out, x, subscr, __args_raw=None:
    out.explode().astype(x.obj.dtype)
)


class _MethodAccessor:
    """Method holder for `_Accessor` objects"""

    def __init__(self, accessor, method):
        self.accessor = accessor
        self.method = method

    def __call__(self, *args, **kwds):
        out = self.accessor.sgb.apply(
            lambda x: getattr(
                getattr(x, self.accessor.name),
                self.method
            )(*args, **kwds)
        )

        try:
            return out.groupby(self.accessor.sgb.grouper)
        except (AttributeError, ValueError, TypeError):  # pragma: no cover
            return out


class _Accessor:
    """Accessor for special columns, such as `.str`, `.cat` and `.dt`, etc

    This is used for SeriesGroupBy object, since `sgb.str` cannot be evaluated
    immediately.
    """
    def __init__(self, sgb: SeriesGroupBy, name: str):
        self.sgb = sgb
        self.name = name

    def __getitem__(self, key):
        return _MethodAccessor(self, "__getitem__")(key)

    def __getattr__(self, name):
        # See if name is a method
        accessor = getattr(Series, self.name)  # Series.str
        attr_or_method = getattr(accessor, name, None)

        if callable(attr_or_method):
            # x.str.lower()
            return _MethodAccessor(self, name)

        # x.cat.categories
        out = self.sgb.apply(
            lambda x: getattr(getattr(x, self.name), name)
        )

        try:
            return out.groupby(self.sgb.grouper)
        except (AttributeError, ValueError, TypeError):  # pragma: no cover
            return out


@func_factory("agg", "x")
def attrgetter(x, attr):
    """Attrgetter as a function for verb

    This is helpful when we want to access to an accessor
    (ie. CategoricalAccessor) from a SeriesGroupBy object
    """
    return getattr(x, attr)


@attrgetter.register(SeriesGroupBy, meta=False)
def _(x, attr):
    return _Accessor(x, attr)


@register_func(None, context=Context.EVAL)
def pd_str(x):
    """Pandas' str accessor for a Series (x.str)

    This is helpful when x is a SeriesGroupBy object
    """
    return regcall(attrgetter, x, "str")


@register_func(None, context=Context.EVAL)
def pd_cat(x):
    """Pandas' cat accessor for a Series (x.cat)

    This is helpful when x is a SeriesGroupBy object
    """
    return regcall(attrgetter, x, "cat")


@register_func(None, context=Context.EVAL)
def pd_dt(x):
    """Pandas' dt accessor for a Series (x.dt)

    This is helpful when x is a SeriesGroupBy object
    """
    return regcall(attrgetter, x, "dt")
