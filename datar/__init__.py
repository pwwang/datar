"""Load operator, provide f and __version__"""
# pylint: disable=unused-import
from typing import Mapping

from .core import operator as _
from .core import _frame_format_patch
from .core.defaults import f

__all__ = ('f', 'datar_versions')
__version__ = "0.4.0"

def datar_versions() -> Mapping[str, str]: # pragma: no cover
    """Get related versions which help for bug reporting."""

    import pandas
    import pipda
    import varname
    from diot import Diot

    return Diot(
        pandas=pandas.__version__,
        pipda=pipda.__version__,
        varname=varname.__version__,
    )
