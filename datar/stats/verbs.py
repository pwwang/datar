from typing import Iterable

from pandas import DataFrame
from pipda import register_verb

from ..dplyr.rename import rename

@register_verb(DataFrame)
def setNames(obj: DataFrame, names: Iterable[str]) -> DataFrame:
    return rename(obj, **dict(zip(names, obj.columns)))
