"""Plugin system to support different backends"""
from typing import Any, List, Mapping, Tuple, Callable

from simplug import Simplug, SimplugResult, makecall

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


@plugin.spec(result=SimplugResult.TRY_SINGLE)
def load_dataset(name: str, metadata: Mapping):
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
def misc_api():
    """What is implemented the misc APIs."""


@plugin.spec(result=SimplugResult.SINGLE)
def c_getitem(item):
    """Get item for c"""


@plugin.spec(result=SimplugResult.SINGLE)
def operate(op: str, x: Any, y: Any = None):
    """Operate on x and y"""
