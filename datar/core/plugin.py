"""Plugin system to support different backends"""
from typing import Any, List, Mapping

from simplug import Simplug, SimplugResult

from .options import get_option

plugin = Simplug("datar")


def _collect(results: List[Mapping[str, Any]]) -> Mapping[str, Any]:
    """Collect the results from plugins"""
    collected = {}
    for result in results:
        if result is not None:
            collected.update(result)
    return collected


@plugin.spec
def setup():
    """Initialize the backend"""


@plugin.spec(collect=_collect)
def get_versions():
    """Return the versions of the dependencies of the plugin."""


@plugin.spec
def data_api():
    """Implementations for load_dataset()"""


@plugin.spec(collect=_collect)
def base_api():
    """What is implemented the base APIs."""


@plugin.spec(collect=_collect)
def dplyr_api():
    """What is implemented the dplyr APIs."""


@plugin.spec(collect=_collect)
def tibble_api():
    """What is implemented the tibble APIs."""


@plugin.spec(collect=_collect)
def forcats_api():
    """What is implemented the forcats APIs."""


@plugin.spec(collect=_collect)
def tidyr_api():
    """What is implemented the tidyr APIs."""


@plugin.spec(collect=_collect)
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
