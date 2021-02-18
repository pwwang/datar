"""Some utilities of group by"""
import warnings

from plyrda.utils import list_union
from typing import Iterable, List, Union
from pandas import DataFrame

GROUP_BY_ATTR = '__plyrda_groupby__'
ROWWISE_ATTR = '__plyrda_rowwise__'

def is_grouped(df: DataFrame) -> bool:
    """Check if a dataframe is grouped"""
    if not hasattr(df, GROUP_BY_ATTR):
        return False

    return bool(getattr(df, GROUP_BY_ATTR))

def set_groups(
        df: DataFrame,
        groups: List[str],
        _add: bool = False,
        # _drop: Optional[bool] = None # not supported yet
) -> None:
    """Set group_by for a dataframe."""
    if _add and is_grouped(df):
        setattr(
            df,
            GROUP_BY_ATTR,
            list_union(getattr(df, GROUP_BY_ATTR, groups))
        )
    else:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            setattr(df, GROUP_BY_ATTR, groups)

def get_groups(df: DataFrame) -> List[str]:
    """Get the groups"""
    if not is_grouped(df):
        return []
    return getattr(df, GROUP_BY_ATTR)

def set_rowwise(
        df: DataFrame,
        rowwise: Union[bool, Iterable[str]] = True
) -> None:
    setattr(df, ROWWISE_ATTR, rowwise)

def get_rowwise(df: DataFrame) -> Union[bool, List[str]]:
    if not hasattr(df, ROWWISE_ATTR):
        return False
    return getattr(df, ROWWISE_ATTR)
