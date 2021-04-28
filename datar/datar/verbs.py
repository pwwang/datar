"""Specific verbs from this package"""
import builtins
from typing import Any, List

from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from pipda import register_verb

from ..core.types import DataFrameType, is_scalar
from ..core.contexts import Context
from ..dplyr import select, slice # pylint: disable=redefined-builtin

@register_verb((DataFrame, DataFrameGroupBy), context=Context.SELECT)
def get(
        _data: DataFrameType,
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
    data = _data.copy()
    # getting single element
    if (
            rows is not None and
            cols is not None and
            is_scalar(rows) and
            is_scalar(cols)
    ):
        if isinstance(rows, str): # index
            rows = data.index.get_indexer_for([rows])[0]
        if isinstance(cols, str):
            cols = data.columns.get_indexer_for([cols])[0]

        return data.iloc[rows, cols]

    if cols is not None:
        data = select(data, cols)

    if rows is not None:
        # slice only support integer index
        if not isinstance(rows, builtins.slice):
            if is_scalar(rows):
                rows = [rows]
            if not isinstance(rows[0], int):
                rows = data.index.get_indexer_for(rows)
        data = slice(data, rows)
    return data

@register_verb(DataFrame)
def flatten(_data: DataFrameType, bycol: bool = False) -> List[Any]:
    """Flatten a dataframe into a 1-d python list

    Args:
        _data: The dataframe

    Returns:
        The flattened list
    """
    if bycol:
        return _data.T.values.flatten().tolist()
    return _data.values.flatten().tolist()


# @register_verb(DataFrame, context=Context.UNSET)
# def debug(
#         _data: DataFrameType,
#         *args: Any,
#         context: Union[Context, str] = Context.SELECT,
#         **kwargs: Any
# ) -> None: # not going any further
#     """Debug the expressions in the argument of the verb

#     Args:
#         _data: the dataframe
#         *args, **kwargs: The pipda expressions
#         context: How the expressions should be evaluated
#     """
#     def print_msg(msg: str, end: str = "\n"):
#         sys.stderr.write(f"[datar] DEBUG: {msg}{end}")

#     if isinstance(_data, DataFrameGroupBy):
#         print_msg("# DataFrameGroupBy:")
#         print_msg(_data.describe())
#     else:
#         print_msg("# DataFrame:")
#         print_msg(_data)

#     if args:
#         for i, arg in enumerate(args):
#             print_msg(f"# Arg#{i+1}")
#             print_msg("## Raw")
#             print_msg(arg)
#             print_msg("## Evaluated")
#             print_msg(evaluate_expr(arg, _data, context))

#     if kwargs:
#         for key, val in kwargs.items():
#             print_msg(f"# Kwarg#{key}")
#             print_msg("## Raw")
#             print_msg(val)
#             print_msg("## Evaluated")
#             print_msg(evaluate_expr(val, _data, context))

@register_verb(DataFrame)
def drop_index(_data: DataFrame) -> DataFrame:
    """Drop the index of a dataframe, works as a verb"""
    return _data.reset_index(drop=True)
