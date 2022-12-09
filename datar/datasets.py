# pragma: no cover
import warnings


class DatasetsDeprecatedWarning(DeprecationWarning):
    ...


warnings.simplefilter("always", DatasetsDeprecatedWarning)

warnings.warn(
    "Import data from `datar.datasets` is deprecated and "
    "will be removed in the future. try `datar.data` instead.",
    DatasetsDeprecatedWarning,
)


def __getattr__(name: str):
    from . import data
    return getattr(data, name)
