"""Count observations by group

See souce code https://github.com/tidyverse/dplyr/blob/master/R/count-tally.R
"""
from datar.core.grouped import DataFrameGroupBy
from typing import Any, Iterable, Optional, Union

from pandas import DataFrame
from pipda import register_verb, evaluate_expr
from pipda.utils import Expression

from ..core.contexts import Context
from ..core.types import NumericOrIter, NumericType
from ..core.utils import logger
from ..core.defaults import f
from .group_by import group_by_drop_default, group_by
from .group_data import group_data, group_size, group_vars
from .mutate import mutate
from .summarise import summarise
from .arrange import arrange
from .desc import desc

@register_verb(DataFrame, context=Context.PENDING)
def count(
        x: DataFrame,
        *args: Any,
        wt: Optional[NumericOrIter] = None,
        sort: bool = False,
        name: Optional[str] = None,
        _drop: Optional[bool] = None,
        **kwargs: Any
) -> DataFrame:
    """Count observations by group

    See https://dplyr.tidyverse.org/reference/count.html

    Args:
        x: The dataframe
        *columns: and
        **mutates: Variables to group by
        wt: Frequency weights. Can be None or a variable:
            If None (the default), counts the number of rows in each group.
            If a variable, computes sum(wt) for each group.
        sort: If TRUE, will show the largest groups at the top.
        name: The name of the new column in the output.

    Returns:
        DataFrame object with the count column
    """
    if _drop is None:
        _drop = group_by_drop_default(x)

    if args or kwargs:
        out = group_by(x, *args, **kwargs, _add=True, _drop=_drop)
    else:
        out = x

    out = tally(out, wt=wt, sort=sort, name=name)
    return out

@register_verb(DataFrame, context=Context.PENDING)
def tally(
        x: DataFrame,
        wt: Optional[NumericOrIter] = None,
        sort: bool = False,
        name: Optional[str] = None
) -> DataFrame:
    """A ower-level function for count that assumes you've done the grouping

    See count()
    """
    n = tally_n(x, wt)

    name = check_name(name, group_vars(x))
    # TODO: thread-safety
    summarise_inform = summarise.inform
    summarise.inform = False
    out = summarise.dispatch(DataFrame)(x, {name: n})
    summarise.inform = summarise_inform

    if isinstance(x, DataFrameGroupBy):
        out = DataFrameGroupBy(
            out,
            _group_vars=group_vars(x),
            _drop=group_by_drop_default(x),
            _group_data=group_data(x)
        )

    if sort:
        return arrange(out, desc(f[name]))
    return out

@register_verb(DataFrame, context=Context.PENDING)
def add_count(
        x: DataFrame,
        *args: Any,
        wt: Optional[str] = None,
        sort: bool = False,
        name: str = 'n',
        **kwargs: Any
) -> DataFrame:
    """Equivalents to count() but use mutate() instead of summarise()

    See count().
    """
    if args or kwargs:
        out = group_by(x, *args, **kwargs, _add=True)
    else:
        out = x

    out = add_tally(out, wt=wt, sort=sort, name=name)
    return out

@register_verb(DataFrame, context=Context.PENDING)
def add_tally(
        x: DataFrame,
        wt: Optional[str] = None,
        sort: bool = False,
        name: str = 'n'
) -> DataFrame:
    """Equivalents to tally() but use mutate() instead of summarise()

    See count().
    """
    wt = evaluate_expr(wt, x, Context.EVAL)
    n = tally_n(x, wt)
    name = check_name(name, x.columns)
    out = mutate(x, {name: n})

    if sort:
        return arrange(out, desc(f[name]))
    return out


# Helpers -----------------------------------------------------------------
def tally_n(
        x: DataFrame,
        wt: Optional[Union[NumericOrIter, Expression]]
) -> Iterable[NumericType]:
    """Compuate the weights for counting"""
    if wt is None:
        return group_size(x)
    if not isinstance(wt, Expression):
        return sum(wt)
    # Expression
    wt = evaluate_expr(wt, x, Context.EVAL)
    return map(lambda rows: wt[rows].sum(), group_data(x)['_rows'])

def check_name(name: Optional[str], invars: Iterable[str]) -> str:
    """Check if count is valid"""
    if name is None:
        name = n_name(invars)

        if name != 'n':
            logger.warning(
                "Storing counts in `%s`, as `n` already present in input. "
                "Use `name=\"new_name\" to pick a new name.`"
            )
    elif not isinstance(name, str):
        raise ValueError("`name` must be a single string.")

    return name


def n_name(invars: Iterable[str]) -> str:
    """Make sure that name does not exist in invars"""
    name = 'n'
    while name in invars:
        name = 'n' + name
    return name
