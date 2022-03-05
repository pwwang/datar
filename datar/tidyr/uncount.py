"""Uncount a data frame"""

from typing import Any, Iterable

from pandas import DataFrame
from pandas.api.types import is_scalar, is_number
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import regcall
from ..core.tibble import reconstruct_tibble

from ..dplyr import group_by, mutate, row_number, ungroup

INDEX_COLUMN = "_UNCOUND_INDEX_"


@register_verb(DataFrame, context=Context.EVAL)
def uncount(
    data: DataFrame,
    weights,
    _remove: bool = True,
    _id: str = None,
) -> DataFrame:
    """Duplicating rows according to a weighting variable

    Args:
        data: A data frame
        weights: A vector of weights. Evaluated in the context of data
        _remove: If TRUE, and weights is the name of a column in data,
            then this column is removed.
        _id: Supply a string to create a new variable which gives a
            unique identifier for each created row (0-based).

    Returns:
        dataframe with rows repeated.
    """
    if is_scalar(weights):
        weights = [weights] * data.shape[0]  # type: ignore

    _check_weights(weights)

    indexes = [
        idx for i, idx in enumerate(data.index) for _ in range(int(weights[i]))
    ]

    all_columns = data.columns
    weight_name = getattr(weights, "name", None)
    if weight_name in all_columns and weights is data[weight_name]:
        rest_columns = all_columns.difference([weight_name])
    else:
        rest_columns = all_columns

    out = data.loc[indexes, rest_columns] if _remove else data.loc[indexes, :]
    # need the indexes to get the right id column
    out = out.assign(**{INDEX_COLUMN: indexes})
    out.reset_index(drop=True, inplace=True)

    if _id:

        out = regcall(
            ungroup,
            mutate(
                regcall(group_by, out, INDEX_COLUMN),
                **{_id: row_number() - 1},
            ),
        )

    out.drop(columns=[INDEX_COLUMN], inplace=True)
    return reconstruct_tibble(data, out)


def _check_weights(weights: Iterable[Any]) -> None:
    """Check if uncounting weights are valid"""
    for weight in weights:
        if not is_number(weight):
            raise ValueError("`weights` must evaluate to numerics.")
        if weight < 0:
            raise ValueError("All elements in `weights` must be >= 0.")
