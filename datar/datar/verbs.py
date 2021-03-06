"""Specific verbs from this package"""
import sys
from typing import Any, List, Union

from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from pipda import register_verb, Context, evaluate_expr

from ..core.utils import objectize
from ..core.types import DataFrameType
from ..dplyr import select, slice

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def get(
        _data: Union[DataFrame, DataFrameGroupBy],
        rows: Any = None,
        cols: Any = None
) -> Any:
    """Get a single element or a subset of a dataframe

    Args:
        _data: The dataframe
        rows: The rows to subset the dataframe
        cols: The columns to subset the dataframe
            If both rows and cols are scalar, then a single element will be
            returned

    Returns:
        A single element when both rows and cols are scalar, otherwise
        a subset of _data
    """
    _data = objectize(_data)
    # getting single element
    if (
            rows is not None and
            cols is not None
    ):
        return _data.loc[rows, cols]

    if rows is not None:
        _data = slice(_data, rows)
    if cols is not None:
        _data = select(_data, cols)
    return _data

@register_verb((DataFrame, DataFrameGroupBy))
def flatten(_data: DataFrameType) -> List[Any]:
    """Flatten a dataframe into a 1-d python list

    Args:
        _data: The dataframe

    Returns:
        The flattened list
    """
    return objectize(_data).values.flatten().tolist()


@register_verb((DataFrame, DataFrameGroupBy), context=Context.UNSET)
def debug(
        _data: DataFrameType,
        *args: Any,
        context: Union[Context, str] = Context.SELECT,
        **kwargs: Any
) -> None: # not going any further
    """Debug the expressions in the argument of the verb

    Args:
        _data: the dataframe
        *args, **kwargs: The pipda expressions
        context: How the expressions should be evaluated
    """
    def print_msg(msg: str, end: str = "\n"):
        sys.stderr.write(f"[datar] DEBUG: {msg}{end}")

    if isinstance(_data, DataFrameGroupBy):
        print_msg("# DataFrameGroupBy:")
        print_msg(_data.describe())
    else:
        print_msg("# DataFrame:")
        print_msg(_data)

    if args:
        for i, arg in enumerate(args):
            print_msg(f"# Arg#{i+1}")
            print_msg("## Raw")
            print_msg(arg)
            print_msg("## Evaluated")
            print_msg(evaluate_expr(arg, _data, context))

    if kwargs:
        for key, val in kwargs.items():
            print_msg(f"# Kwarg#{key}")
            print_msg("## Raw")
            print_msg(val)
            print_msg("## Evaluated")
            print_msg(evaluate_expr(val, _data, context))
