"""Specific verbs from this package"""
from typing import Any, List

from pipda import register_verb

from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.api.types import is_scalar

from ..core.contexts import Context
from ..dplyr import select, slice_


@register_verb(DataFrame, context=Context.SELECT)
def get(
    _data: DataFrame,
    rows: Any = None,
    cols: Any = None,
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
    if rows is None and cols is None:
        return _data.copy()

    # getting single element
    if (
        rows is not None
        and cols is not None
        and is_scalar(rows)
        and is_scalar(cols)
    ):
        if isinstance(rows, str):  # index
            rows = _data.index.get_indexer_for([rows])[0]

        if isinstance(cols, str):
            cols = _data.columns.get_indexer_for([cols])[0]

        return _data.iloc[rows, cols]

    if cols is not None:
        _data = select(_data, cols, __ast_fallback="normal")

    if rows is not None:
        # slice only support integer index
        if not isinstance(rows, slice):
            if is_scalar(rows):
                rows = [rows]
            if not isinstance(rows[0], int):
                rows = _data.index.get_indexer_for(rows)

        _data = slice_(_data, rows, __ast_fallback="normal")
    return _data


@register_verb(DataFrame)
def flatten(_data: DataFrame, bycol: bool = False) -> List[Any]:
    """Flatten a dataframe into a 1-d python list

    Args:
        _data: The dataframe

    Returns:
        The flattened list
    """
    if bycol:
        return _data.T.values.flatten().tolist()
    return _data.values.flatten().tolist()
