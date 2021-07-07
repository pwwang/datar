"""Uncount a data frame"""

from typing import Any, Iterable

import numpy
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.types import IntOrIter, is_scalar
from ..core.utils import get_option, reconstruct_tibble

from ..dplyr import group_by, mutate, row_number, ungroup

INDEX_COLUMN = "_UNCOUND_INDEX_"


@register_verb(DataFrame, context=Context.EVAL)
def uncount(
    data: DataFrame,
    weights: IntOrIter,
    _remove: bool = True,
    _id: str = None,
    base0_: bool = None,
) -> DataFrame:
    """Duplicating rows according to a weighting variable

    Args:
        data: A data frame
        weights: A vector of weights. Evaluated in the context of data
        _remove: If TRUE, and weights is the name of a column in data,
            then this column is removed.
        _id: Supply a string to create a new variable which gives a
            unique identifier for each created row (0-based).
        base0_: Whether the generated `_id` columns are 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        dataframe with rows repeated.
    """
    if is_scalar(weights):
        weights = [weights] * data.shape[0] # type: ignore

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
        base = int(not get_option("index.base.0", base0_))
        # pylint: disable=no-value-for-parameter
        out = (
            out
            >> group_by(INDEX_COLUMN)
            >> mutate(**{_id: row_number() + base - 1})
            >> ungroup()
        )

    out.drop(columns=[INDEX_COLUMN], inplace=True)
    return reconstruct_tibble(data, out)


def _check_weights(weights: Iterable[Any]) -> None:
    """Check if uncounting weights are valid"""
    for weight in weights:
        if not isinstance(weight, (int, float, numpy.number)):
            raise ValueError("`weights` must evaluate to numerics.")
        if weight < 0:
            raise ValueError("All elements in `weights` must be >= 0.")
