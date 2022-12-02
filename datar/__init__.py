from typing import Mapping as _Mapping

from .core import operator as _
from .core.defaults import f
from .core.options import options, get_option, options_context

__version__ = "0.10.0"


def get_versions(prnt: bool = True) -> _Mapping[str, str]:
    """Return/Print the versions of the dependencies.

    Args:
        prnt: If True, print the versions, otherwise return them.

    Returns:
        A dict of the versions of the dependencies if `prnt` is False.
    """
    import sys
    import executing
    import pipda
    import simplug
    from .core.plugin import plugin

    versions = {
        "python": sys.version,
        "datar": __version__,
        "simplug": simplug.__version__,
        "executing": executing.__version__,
        "pipda": pipda.__version__,
    }

    versions_plg = plugin.hooks.get_versions()
    versions.update(versions_plg)

    if not prnt:
        return versions

    keylen = max(map(len, versions))
    for key in versions:
        ver = versions[key]
        verlines = ver.splitlines()
        print(f"{key.ljust(keylen)}: {verlines.pop(0)}")
        for verline in verlines:
            print(f"{' ' * keylen}  {verline}")

    return None
