"""Provide some helper functions"""
import re
import inspect
from typing import Any, Callable, Iterable, List, Union

import numpy
import pandas
from pandas import DataFrame
from pandas.core.series import Series
from pipda.context import Context
from pipda.utils import Expression
from pipda.symbolic import DirectRefAttr, DirectRefItem
from varname import argname
from varname.core import varname

from .utils import to_df

def tibble(
        *args: Any,
        _name_repair: Union[str, Callable] = 'check_unique',
        **kwargs: Any
) -> DataFrame:
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
                        int(new_name[len(name)+2:])
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
            return repair_name(name, 'unique')

        if callable(name_repair):
            if len(inspect.signature(name_repair).parameters) == 2:
                new_name = name_repair(name, raw_names)
            else:
                new_name = name_repair(name)

            raw_names.append(name)
            new_names.append(new_name)
            return new_name

        if isinstance(name_repair, Iterable):
            tmpname = f'_tmp_{len(raw_names)}'
            raw_names.append(tmpname)
            if not new_names:
                new_names.extend(name_repair)
            return tmpname

        raise ValueError(
            "Expect 'minimal', 'unique', 'check_unique', "
            "'universal', callable or a list of names for '_name_repair', "
            f"but got {type(name_repair)!r}"
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

def runif(n: int, min: float = 0.0, max: float = 1.0) -> List[float]:
    return numpy.random.uniform(low=min, high=max, size=n)

def rnorm(n: int, mean: float = 0.0, var: float = 1.0) -> List[float]:
    return numpy.random.normal(loc=mean, scale=var, size=n)
