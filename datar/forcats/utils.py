from pandas import Categorical
from ..core.types import ForcatsType, is_categorical

def check_factor(_f: ForcatsType):
    """Make sure the input become a factor"""
    if not is_categorical(_f):
        return Categorical(_f)

    return _f
