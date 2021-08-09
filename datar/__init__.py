"""Load operator, provide f and __version__"""
# pylint: disable=unused-import
from typing import Mapping

from .core import operator as _
from .core import _frame_format_patch
from .core.defaults import f

__all__ = ('f', 'get_versions')
__version__ = "0.4.3"

def get_versions(
    prnt: bool = True
) -> Mapping[str, str]: # pragma: no cover
    """Print or return related versions which help for bug reporting.

    Args:
        prnt: Print the versions instead of returning them?

    Returns:
        A `Diot` object of the versions.
    """
    import sys

    import numpy
    import pandas
    import pipda
    import executing
    import varname
    from diot import Diot

    out = Diot(
        python=sys.version,
        datar=__version__,
        numpy=numpy.__version__,
        pandas=pandas.__version__,
        pipda=pipda.__version__,
        executing=executing.__version__,
        varname=varname.__version__,
    )
    if not prnt:
        return out

    keylen = max(map(len, out))
    for key, ver in out.items():
        verlines = ver.splitlines()
        print(f"{key.ljust(keylen)}: {verlines.pop(0)}")
        for verline in verlines:
            print(f"{' ' * keylen}  {verline}")

    return None
