"""Functions help to create data frames"""

from typing import Any, Union, Callable, Mapping, Iterable, Tuple

from pandas import DataFrame
from varname import argname2, varname, VarnameRetrievingError

import pipda
from pipda import register_func, evaluate_expr
from pipda.expression import Expression
from pipda.symbolic import DirectRefItem, DirectRefAttr

from ..core.types import Dtype, is_null
from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.collections import Collection
from ..core.contexts import Context
from ..core.names import repair_names
from ..core.utils import (
    Array,
    to_df,
    length_of,
    recycle_df,
    df_setitem,
    copy_attrs,
    apply_dtypes,
)


@register_func(None, context=Context.EVAL)
def tibble(
    *args: Any,
    _name_repair: Union[str, Callable] = "check_unique",
    _rows: int = None,
    base0_: bool = None,
    dtypes_: Union[Dtype, Mapping[str, Dtype]] = None,
    frame_: int = 1,
    **kwargs: Any,
) -> DataFrame:
    """Constructs a data frame

    Args:
        *args: and
        **kwargs: A set of name-value pairs.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        _rows: Number of rows of a 0-col dataframe when args and kwargs are
            not provided. When args or kwargs are provided, this is ignored.
        base0_: Whether the suffixes of repaired names should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`.

    Returns:
        A constructed dataframe
    """
    args = tuple((arg for arg in args if arg is not None))
    if not args and not kwargs:
        df = DataFrame() if not _rows else DataFrame(index=range(_rows))
        df.__dfname__ = varname(raise_exc=False, ignore=pipda)
        return df

    names = [None] * len(args)
    values = list(args)
    try:
        argnames = argname2(
            "*args",
            frame=frame_,
            ignore=pipda,
            func=tibble,
            vars_only=False,
        )
        if len(argnames) != len(args):
            raise VarnameRetrievingError
    except VarnameRetrievingError:
        argnames = None

    if argnames:
        for i, value in enumerate(values):
            if isinstance(value, Expression):
                names[i] = argnames[i]
            elif _expand_value(value) is None and not getattr(
                value, "__name__", getattr(value, "name", None)
            ):
                names[i] = argnames[i]

    names.extend(kwargs)
    values.extend(kwargs.values())

    out = zibble(
        names, values, _name_repair=_name_repair, base0_=base0_, dtypes_=dtypes_
    )

    out.__dfname__ = varname(raise_exc=False, frame=frame_, ignore=pipda)

    if not kwargs and len(args) == 1 and isinstance(args[0], DataFrame):
        copy_attrs(out, args[0])

    return out


@register_func(None, context=Context.EVAL)
def tibble_row(
    *args: Any,
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
    dtypes_: Union[Dtype, Mapping[str, Dtype]] = None,
    **kwargs: Any,
) -> DataFrame:
    """Constructs a data frame that is guaranteed to occupy one row.

    Scalar values will be wrapped with `[]`

    Args:
        *args: and
        **kwargs: A set of name-value pairs.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        base0_: Whether the suffixes of repaired names should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`.

    Returns:
        A constructed dataframe
    """
    if not args and not kwargs:
        df = DataFrame(index=[0])  # still one row
    else:
        df = tibble(
            *args, **kwargs, _name_repair=_name_repair, base0_=base0_, frame_=2
        )

    if df.shape[0] > 1:
        raise ValueError("All arguments must be size one, use `[]` to wrap.")
    try:
        df.__dfname__ = varname(raise_exc=False)
    except VarnameRetrievingError:  # pragma: no cover
        df.__dfname__ = None

    apply_dtypes(df, dtypes_)
    return df


def tribble(
    *dummies: Any,
    _name_repair: Union[str, Callable] = "minimal",
    base0_: bool = None,
    dtypes_: Union[Dtype, Mapping[str, Dtype]] = None,
) -> DataFrame:
    """Create dataframe using an easier to read row-by-row layout

    Unlike original API that uses formula (`f.col`) to indicate the column
    names, we use `f.col` to indicate them.

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
    len_data = 0
    for dummy in dummies:
        # columns
        if isinstance(dummy, (DirectRefAttr, DirectRefItem)):
            columns.append(dummy._pipda_ref)
        elif not columns:
            raise ValueError(
                "Must specify at least one column using the `f.<name>` syntax."
            )
        else:
            if not data:
                data.append([])
            if len(data[-1]) < len(columns):
                data[-1].append(dummy)
                len_data += 1
            else:
                data.append([dummy])
                len_data += 1

    if len_data == 0:
        return zibble(
            columns,
            [[]] * len(columns),
            _name_repair=_name_repair,
            base0_=base0_,
            dtypes_=dtypes_,
        )

    if len_data % len(columns) != 0:
        raise ValueError(
            "Data must be rectangular. "
            f"{len_data} cells is not an integer multiple of "
            f"{len(columns)} columns."
        )

    return zibble(
        columns,
        list(zip(*data)),
        _name_repair=_name_repair,
        base0_=base0_,
        dtypes_=dtypes_,
    )


def zibble(
    names: Iterable[str],
    values: Iterable,
    _name_repair: Union[str, Callable] = "minimal",
    base0_: bool = None,
    dtypes_: Union[Dtype, Mapping[str, Dtype]] = None,
) -> DataFrame:
    """Zip version of tibble, where names specify together and so do values.

    So that it is easlier to create data frame with duplicated names.

    The earlier create columns can be recycled by later expressions.
    For example, `zibble(['x', 'y'], [1, f.x])`. You can also use expressions
    to specify names: `zibble([f.x, f.y], [1, f.x])`.

    Note that for `values`, do not use `c()` to quote all the values, as they
    will be flattened.

    Args:
        names: The names of the columns
            If a name is None/NULL/NA, we should expand the corresponding value
            to get the name
        values: The values for the columns
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        base0_: Whether the suffixes of repaired names should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`.

    Returns:
        A data frame
    """
    if len(names) != len(values):
        raise ValueError("Lengths of `names` and `values` are not the same.")

    names = Array(
        [
            name._pipda_ref
            if isinstance(DirectRefAttr, DirectRefItem)
            else name
            for name in names
        ],
        dtype=object,
    )

    out = None
    suffix = 0
    for name, value in zip(names, values):
        # Evaluate value if neccessary

        value = evaluate_expr(value, out, Context.EVAL)
        if isinstance(value, Collection):
            value.expand()
        expanded = _expand_value(value)

        if is_null(name) and expanded is None:
            name = getattr(
                value,
                "__name__",
                getattr(value, "name", f"{DEFAULT_COLUMN_PREFIX}{suffix}"),
            )
            out = _process_one_pair(out, name, value)
        elif is_null(name):
            for nam, val in zip(*expanded):
                out = _process_one_pair(out, nam, val)
        elif expanded is None:
            out = _process_one_pair(out, name, value)
        else:
            sub = None
            for nam, val in zip(*expanded):
                sub = _process_one_pair(sub, nam, val)
            out = _process_one_pair(out, name, sub)

        suffix += 1

    if out is None:
        out = DataFrame()
    names = repair_names(out.columns.tolist(), _name_repair, base0_)
    out.columns = names
    apply_dtypes(out, dtypes_)

    return out


# Helpers ----------------------------------------------------
def _process_one_pair(df: DataFrame, name: str, value: Any) -> DataFrame:
    """Process one name-value pair"""
    if value is None:
        return df

    if df is None:
        # if it is expanded, it should be series
        # so the name must be available
        if isinstance(value, DataFrame):
            # value could be a core.DataFrameGroupBy object
            df = DataFrame(value, copy=True)
            df.columns = [f"{name}${col}" for col in df.columns]
            return df
        return to_df(value, name)

    if length_of(df) == 1:
        df, value = recycle_df(df, value)

    return df_setitem(df, name, value, allow_dups=True)


def _expand_value(value: Any) -> Tuple[str, Iterable[Any]]:
    """Expand value to name-value pairs"""
    if isinstance(value, DataFrame):
        df_dict = value.to_dict("series")
        return df_dict, df_dict.values()
    if isinstance(value, dict):
        return value, value.values()
    return None
