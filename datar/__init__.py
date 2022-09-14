from .core import operator as _
from .core import f, options_context, options, add_option, get_option, logger
from .core.options import apply_init_callbacks

__all__ = (
    "f",
    "options",
    "get_versions",
    "options_context",
    "add_option",
    "get_option",
    "logger",
)

__all__ = ("f", "get_versions")
__version__ = "0.9.0"

apply_init_callbacks()


def get_versions(prnt: bool = True):
    """Print or return related versions which help for bug reporting.

    Args:
        prnt: Print the versions instead of returning them?

    Returns:
        A `Diot` object of the versions.
    """
    import sys

    import numpy
    import pipda
    import executing
    from diot import Diot

    out = Diot(
        python=sys.version,
        datar=__version__,
        numpy=numpy.__version__,
        pipda=pipda.__version__,
        executing=executing.__version__,
    )

    backend = get_option("backend")
    if backend == "pandas":
        import pandas
        out["pandas"] = pandas.__version__
    elif backend == "modin":  # pragma: no cover
        import modin
        out["modin"] = modin.__version__

    if not prnt:
        return out

    keylen = max(map(len, out))
    for key in out:
        ver = getattr(out, key)
        verlines = ver.splitlines()
        print(f"{key.ljust(keylen)}: {verlines.pop(0)}")
        for verline in verlines:
            print(f"{' ' * keylen}  {verline}")

    return None
