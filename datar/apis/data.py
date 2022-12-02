from typing import Any, TYPE_CHECKING

from pipda import register_func

if TYPE_CHECKING:
    from ..data import Metadata


@register_func(dispatchable="first")
def load_dataset(name: str, meta: "Metadata") -> Any:
    """Load a dataset from the registry

    Args:
        name: The name of the dataset

    Returns:
        The dataset
    """
    return None
