from typing import Iterable

from pandas import DataFrame
from pipda import register_verb

@register_verb(DataFrame)
def setNames(obj: DataFrame, names: Iterable[str]) -> DataFrame:
    out = obj.copy()
    out.columns = names
    return out
