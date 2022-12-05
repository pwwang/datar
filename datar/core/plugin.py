"""Plugin system to support different backends"""
from typing import Any, List, Mapping, Tuple, Callable

from simplug import Simplug, SimplugResult, makecall

from .options import get_option

plugin = Simplug("datar")


def _collect(calls: List[Tuple[Callable, Tuple, Mapping]]) -> Mapping[str, Any]:
    """Collect the results from plugins"""
    collected = {}
    for call in calls:
        out = makecall(call)
        if out is not None:
            collected.update(out)
    return collected


@plugin.spec
def setup():
    """Initialize the backend"""


@plugin.spec(result=_collect)
def get_versions():
    """Return the versions of the dependencies of the plugin."""


@plugin.spec
def data_api():
    """Implementations for load_dataset()"""


@plugin.spec(result=_collect)
def base_api():
    """What is implemented the base APIs."""


@plugin.spec(result=_collect)
def dplyr_api():
    """What is implemented the dplyr APIs."""


@plugin.spec(result=_collect)
def tibble_api():
    """What is implemented the tibble APIs."""


@plugin.spec(result=_collect)
def forcats_api():
    """What is implemented the forcats APIs."""


@plugin.spec(result=_collect)
def tidyr_api():
    """What is implemented the tidyr APIs."""


@plugin.spec(result=_collect)
def other_api():
    """What is implemented the other APIs."""


@plugin.spec(result=SimplugResult.LAST)
def c_getitem(item):
    """Get item for c"""


@plugin.spec(result=SimplugResult.LAST)
def operate(op, x, y=None):
    """Operate on x and y"""


plugin.load_entrypoints(only=get_option("backends"))

plugin.hooks.setup()
