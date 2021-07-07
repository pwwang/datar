"""Complete a data frame with missing combinations of data

https://github.com/tidyverse/tidyr/blob/HEAD/R/complete.R
"""

from typing import Iterable, Mapping, Any

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import reconstruct_tibble

from ..dplyr import full_join
from .replace_na import replace_na
from .expand import expand


@register_verb(DataFrame, context=Context.PENDING)
def complete(
    data: DataFrame,
    *args: Iterable[Any],
    fill: Mapping[str, Any] = None,
    **kwargs: Iterable[Any],
) -> DataFrame:
    """Turns implicit missing values into explicit missing values.

    Args:
        data: A data frame
        *args: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.

    Returns:
        Data frame with missing values completed
    """
    full = expand(data, *args, **kwargs)
    if full.shape[0] == 0:
        return data.copy()

    full = full_join(full, data, by=full.columns.tolist())
    full = replace_na(full, fill)

    return reconstruct_tibble(data, full)
