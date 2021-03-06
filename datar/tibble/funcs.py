"""Functions ported from tidyverse-tibble"""
import re
import inspect

import pandas
from typing import Any, Callable, Union
from pandas import DataFrame
from varname import argname, varname
from pipda import Context
from pipda.utils import Expression
from pipda.symbolic import DirectRefAttr, DirectRefItem

from ..core.types import is_iterable
from ..core.utils import to_df

def tibble(
        *args: Any,
        _name_repair: Union[str, Callable] = 'check_unique',
        **kwargs: Any
) -> DataFrame:
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

    Returns:
        A dataframe
    """
    # .rows not supported
    argnames = argname(args, vars_only=False)
    df = None

    raw_names = []
    new_names = []

    def repair_name(
            name: str,
            name_repair: Union[str, Callable] = _name_repair
    ) -> str:

        if name_repair == 'minimal':
            raw_names.append(name)
            new_names.append(name)
            return name
        if name_repair == 'unique':
            if name in raw_names:
                if name in new_names:
                    new_names[new_names.index(name)] = f'{name}_1'
                    new_names.append(f'{name}_2')
                else:
                    indexes = [
                        int(new_name[len(name)+1:])
                        for new_name in new_names
                        if new_name.startswith(f'{name}_')
                    ]
                    new_names.append(
                        f'{name}_1' if not indexes
                        else f'{name}_{max(indexes) + 1}'
                    )
            else:
                new_names.append(name)
            raw_names.append(name)
            return new_names[-1]
        if name_repair == 'check_unique':
            if name in raw_names:
                raise ValueError(f"Column name {name!r} duplicated.")
            return repair_name(name, 'minimal')
        if name_repair == 'universal':
            name = re.sub(r'[^a-zA-Z0-9]', '_', name)
            name = re.sub(r'_+', '_', name).rstrip('_')
            return repair_name(name, 'unique')

        if callable(name_repair):
            if len(inspect.signature(name_repair).parameters) == 3:
                new_name = name_repair(name, raw_names, new_names)
            else:
                new_name = name_repair(name)

            raw_names.append(name)
            new_names.append(new_name)
            return new_name

        if is_iterable(name_repair):
            tmpname = f'_tmp_{len(raw_names)}'
            raw_names.append(tmpname)
            if not new_names:
                new_names.extend(name_repair)
            return tmpname

        raise ValueError(
            "Expect 'minimal', 'unique', 'check_unique', "
            "'universal', callable or a list of names for '_name_repair', "
            f"but got {name_repair!r}"
        )

    for name, arg in zip(argnames, args):
        if isinstance(arg, Expression):
            arg = arg.evaluate(df, Context.EVAL.value)

        if df is None:
            df = to_df(arg, repair_name(name))
        elif isinstance(arg, DataFrame):
            arg = DataFrame(
                arg.values,
                columns=[repair_name(col) for col in arg.columns]
            )
            df = pandas.concat([df, arg], axis=1)
        else:
            df[repair_name(name)] = arg

    for key, val in kwargs.items():
        key = repair_name(key)
        if isinstance(val, Expression):
            val = val.evaluate(df, Context.EVAL.value)

        if df is None:
            df = to_df(val, key)
        elif isinstance(val, DataFrame):
            val = DataFrame(
                val.values,
                columns=[f'{key}[{col!r}]' for col in val.columns]
            )
            df = pandas.concat([df, val], axis=1)
        else:
            df[key] = val

    if (
            new_names != df.columns.to_list() and
            _name_repair not in ('minimal', 'check_unique')
    ):
        df = df.rename(columns=dict(zip(df.columns, new_names)))

    df.__dfname__ = varname(raise_exc=False)

    return df

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
    data = [[]]
    for dummy in dummies:
        # columns
        if isinstance(dummy, (DirectRefAttr, DirectRefItem)):
            columns.append(dummy.ref)
        else:
            # columns have been finished
            if len(data[-1]) == len(columns):
                data.append([])
            data[-1].append(dummy)

    ret = DataFrame(data, columns=columns)
    ret.__dfname__ = varname(raise_exc=False)
    return ret
