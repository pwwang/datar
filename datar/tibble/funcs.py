"""Functions ported from tidyverse-tibble"""
import itertools
from typing import Any, Callable, Union, Optional

from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from pipda import Context, register_func
from pipda.utils import Expression
from pipda.symbolic import DirectRefAttr, DirectRefItem
from varname import argname, varname
from varname.utils import VarnameRetrievingError

from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.utils import copy_attrs, df_assign_item, objectize, to_df
from ..core.names import repair_names

def tibble(
        *args: Any,
        _name_repair: Union[str, Callable] = 'check_unique',
        _rows: Optional[int] = None,
        **kwargs: Any
) -> DataFrame:
    # pylint: disable=too-many-statements
    """Constructs a data frame

    Args:
        *args, **kwargs: A set of name-value pairs.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        _rows: Number of rows of a 0-col dataframe when args and kwargs are
            not provided. When args or kwargs are provided, this is ignored.

    Returns:
        A dataframe
    """
    if not args and not kwargs:
        df = DataFrame() if not _rows else DataFrame(index=range(_rows))
        try:
            df.__dfname__ = varname(raise_exc=False)
        except VarnameRetrievingError:
            df.__dfname__ = None
        return df
    try:
        argnames = argname(args, vars_only=False, pos_only=True)
    except VarnameRetrievingError:
        argnames = [f"{DEFAULT_COLUMN_PREFIX}{i}" for i in range(len(args))]

    name_values = zip(argnames, args)
    name_values = itertools.chain(name_values, kwargs.items())
    # cannot do it with Mappings, same keys will be lost
    names = []
    values = []
    for name, value in name_values:
        names.append(name)
        values.append(value)

    names = repair_names(names, repair=_name_repair)
    df = None

    for name, arg in zip(names, values):
        if arg is None:
            continue
        if isinstance(arg, Expression):
            arg = arg(df, Context.EVAL.value)

        if isinstance(arg, dict):
            arg = tibble(**arg)

        if df is None:
            if isinstance(arg, DataFrame):
                # df = arg.copy()
                # DataFrameGroupBy.copy copied into DataFrameGroupBy
                df = DataFrame(arg).copy()
                if name not in argnames:
                    df.columns = [f'{name}${col}' for col in df.columns]

            else:
                df = to_df(arg, name)
        elif isinstance(arg, (DataFrame, DataFrameGroupBy)):
            arg = objectize(arg)
            for col in arg.columns:
                df_assign_item(
                    df,
                    f'{name}${col}' if name not in argnames else col,
                    arg[col],
                    allow_dups=True
                )
        else:
            df_assign_item(df, name, arg, allow_dups=True)

    if df is None:
        df = DataFrame()
    try:
        df.__dfname__ = varname(raise_exc=False)
    except VarnameRetrievingError: # still raises in some cases
        df.__dfname__ = None

    if not kwargs and len(args) == 1 and isinstance(args[0], DataFrame):
        copy_attrs(df, args[0])
    return df

@register_func(None, context=Context.EVAL)
def fibble(
        *args: Any,
        _name_repair: Union[str, Callable] = 'check_unique',
        _rows: Optional[int] = None,
        **kwargs: Any
) -> DataFrame:
    """A function of tibble that can be used as an argument of verbs"""
    return tibble(*args, **kwargs, _name_repair=_name_repair, _rows=_rows)

def tribble(*dummies: Any) -> DataFrame:
    """Create dataframe using an easier to read row-by-row layout

    Args:
        *dummies: Arguments specifying the structure of a dataframe
            Variable names should be specified with `f.name`

    Examples:
        >>> tribble(
        >>>     f.colA, f.colB,
        >>>     "a",    1,
        >>>     "b",    2,
        >>>     "c",    3,
        >>> )

    Returns:
        A dataframe
    """
    columns = []
    data = []
    for dummy in dummies:
        # columns
        if isinstance(dummy, (DirectRefAttr, DirectRefItem)):
            columns.append(dummy.ref)
        elif not columns:
            raise ValueError(
                'Must specify at least one column using the `f.<name>` syntax.'
            )
        else:
            if not data:
                data.append([])
            if len(data[-1]) < len(columns):
                data[-1].append(dummy)
            else:
                data.append([dummy])

    ret = (
        DataFrame(data, columns=columns) if data
        else DataFrame(columns=columns)
    )
    try:
        ret.__dfname__ = varname(raise_exc=False)
    except VarnameRetrievingError:
        ret.__dfname__ = None
    return ret
