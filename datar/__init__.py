# register operator
from collections import namedtuple as _namedtuple

from .core import operator as _
from .core import f, options_context, options, add_option, get_option, logger

__all__ = (
    "f",
    "options",
    "get_versions",
    "options_context",
    "add_option",
    "get_option",
    "logger",
)

options(enable_pdtypes=True)

_VersionsTuple = _namedtuple(
    "_VersionsTuple",
    [
        "python",
        "datar",
        "numpy",
        "pandas",
        "pipda",
        "executing",
        "varname",
    ],
)

__all__ = ("f", "get_versions")
__version__ = "0.6.2"


def get_versions(prnt: bool = True) -> _VersionsTuple:
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

    out = _VersionsTuple(
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

    keylen = max(map(len, out._fields))
    for key in out._fields:
        ver = getattr(out, key)
        verlines = ver.splitlines()
        print(f"{key.ljust(keylen)}: {verlines.pop(0)}")
        for verline in verlines:
            print(f"{' ' * keylen}  {verline}")

    return None
