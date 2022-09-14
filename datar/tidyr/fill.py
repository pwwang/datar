"""Fill in missing values with previous or next value

https://github.com/tidyverse/tidyr/blob/HEAD/R/fill.R
"""

from typing import Union

from ..core.backends.pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import vars_select
from ..core.tibble import TibbleGrouped, reconstruct_tibble


@register_verb(DataFrame, context=Context.SELECT)
def fill(
    _data: DataFrame,
    *columns: Union[str, int],
    _direction: str = "down",
) -> DataFrame:
    """Fills missing values in selected columns using the next or
    previous entry.

    See https://tidyr.tidyverse.org/reference/fill.html

    Args:
        _data: A dataframe
        *columns: Columns to fill
        _direction: Direction in which to fill missing values.
            Currently either "down" (the default), "up",
            "downup" (i.e. first down and then up) or
            "updown" (first up and then down).

    Returns:
        The dataframe with NAs being replaced.
    """
    data = _data.copy()
    if not columns:
        data = data.fillna(
            method="ffill" if _direction.startswith("down") else "bfill",
        )
        if _direction in ("updown", "downup"):
            data = data.fillna(
                method="ffill" if _direction.endswith("down") else "bfill",
            )
    else:
        colidx = vars_select(data.columns, *columns)
        data.iloc[:, colidx] = fill(
            data.iloc[:, colidx],
            _direction=_direction,
            __ast_fallback="normal",
        )
    return data


@fill.register(TibbleGrouped, context=Context.SELECT)
def _(
    _data: TibbleGrouped,
    *columns: str,
    _direction: str = "down",
) -> TibbleGrouped:
    # TibbleGrouped
    out = _data._datar["grouped"].apply(
        fill,
        *columns,
        _direction=_direction,
        __ast_fallback="normal",
        # drop the index, pandas 1.4 and <1.4 act differently
    ).sort_index().reset_index(drop=True)
    return reconstruct_tibble(_data, out)
