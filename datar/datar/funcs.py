"""Basic functions"""
from pandas import Series
from pandas.core.groupby import SeriesGroupBy
from pipda import evaluate_expr
from ..core.factory import func_factory
from ..core.contexts import Context
from ..core.collections import Collection


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
