"""Count observations by group

See souce code https://github.com/tidyverse/dplyr/blob/master/R/count-tally.R
"""
from pandas import DataFrame
from pipda import register_verb
from pipda.function import Function

from ..core.contexts import Context
from ..core.utils import logger, regcall
from ..core.defaults import f
from ..core.tibble import reconstruct_tibble
from ..base import options_context, sum_
from .context import n
from .group_by import group_by_drop_default, group_by, ungroup
from .group_data import group_vars
from .mutate import mutate
from .summarise import summarise
from .arrange import arrange
from .desc import desc


@register_verb(DataFrame, context=Context.PENDING)
def count(
    x,
    *args,
    wt=None,
    sort=False,
    name=None,
    _drop=None,
    **kwargs,
):
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
        out = regcall(
            group_by,
            x,
            *args,
            **kwargs,
            _add=True,
            _drop=_drop,
        )
    else:
        out = x

    out = regcall(
        tally,
        out,
        wt=wt,
        sort=sort,
        name=name,
    )

    return reconstruct_tibble(x, out)


@register_verb(DataFrame, context=Context.PENDING)
def tally(
    x,
    wt=None,
    sort=False,
    name=None,
):
    """A ower-level function for count that assumes you've done the grouping

    See count()
    """
    tallyn = _tally_n(wt)

    name = _check_name(name, regcall(group_vars, x))
    # thread-safety?
    with options_context(dplyr_summarise_inform=False):
        out = regcall(
            summarise,
            x,
            **{
                # name: n(__calling_env=CallingEnvs.PIPING)
                name: Function(n, (), {})
                if tallyn is None
                else tallyn
            },
        )

    if sort:
        out = regcall(
            arrange,
            regcall(ungroup, out),
            # desc(f[name], __calling_env=CallingEnvs.PIPING)
            Function(desc, (f[name], ), {}, dataarg=False)
        )
        out.reset_index(drop=True, inplace=True)
        return reconstruct_tibble(x, out)

    return out


@register_verb(DataFrame, context=Context.PENDING)
def add_count(
    x,
    *args,
    wt=None,
    sort=False,
    name="n",
    **kwargs,
):
    """Equivalents to count() but use mutate() instead of summarise()

    See count().
    """
    if args or kwargs:
        out = regcall(
            group_by,
            x,
            *args,
            **kwargs,
            _add=True,
        )
    else:
        out = x

    out = regcall(add_tally, out, wt=wt, sort=sort, name=name)
    return out


@register_verb(DataFrame, context=Context.PENDING)
def add_tally(x, wt=None, sort=False, name="n"):
    """Equivalents to tally() but use mutate() instead of summarise()

    See count().
    """
    tallyn = _tally_n(wt)
    name = _check_name(name, x.columns)

    out = regcall(
        mutate,
        x,
        **{
            # name: n(__calling_env=CallingEnvs.PIPING)
            name: Function(n, (), {})
            if tallyn is None
            else tallyn
        },
    )

    if sort:
        sort_ed = regcall(
            arrange,
            regcall(ungroup, out),
            # desc(f[name], __calling_env=CallingEnvs.PIPING)
            Function(desc, (f[name], ), {}, dataarg=False)
        )
        sort_ed.reset_index(drop=True, inplace=True)
        return reconstruct_tibble(x, sort_ed)

    return out


# Helpers -----------------------------------------------------------------
def _tally_n(wt):
    """Compuate the weights for counting"""
    if wt is None:
        return None  # will be n() later on

    # If it's Expression, will return a Function object
    # Otherwise, sum of wt
    return Function(sum_, (wt, ), {"na_rm": True}, dataarg=False)
    # return sum_(wt, na_rm=True, __calling_env=CallingEnvs.PIPING)


def _check_name(name, invars):
    """Check if count is valid"""
    if name is None:
        name = _n_name(invars)

        if name != "n":
            logger.warning(
                "Storing counts in `%s`, as `n` already present in input. "
                'Use `name="new_name" to pick a new name.`'
            )
    elif not isinstance(name, str):
        raise ValueError("`name` must be a single string.")

    return name


def _n_name(invars):
    """Make sure that name does not exist in invars"""
    name = "n"
    while name in invars:
        name = "n" + name
    return name
