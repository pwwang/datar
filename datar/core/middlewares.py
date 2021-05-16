"""Middlewares for datar"""
from typing import Any, Mapping, Tuple
from pipda.utils import DataEnv

from .utils import logger

class CurColumn:
    """Current column in across"""
    @classmethod
    def replace_args(cls, args: Tuple[Any], column: str) -> Tuple[Any]:
        """Replace self with the real column in args"""
        return tuple(column if isinstance(arg, cls) else arg for arg in args)

    @classmethod
    def replace_kwargs(
            cls,
            kwargs: Mapping[str, Any],
            column: str
    ) -> Mapping[str, Any]:
        """Replace self with the real column in kwargs"""
        return {
            key: column if isinstance(val, cls) else val
            for key, val in kwargs.items()
        }


class WithDataEnv:
    """Implements `with data` to mimic R's `with(data, ...)`"""
    def __init__(self, data: Any) -> None:
        self.data = DataEnv(data)

    def __enter__(self) -> Any:
        return self.data

    def __exit__(self, *exc_info) -> None:
        self.data.delete()

class Nesting:
    """Nesting objects for calls from tidyr.nesting"""
    def __init__(self, *columns: Any, **kwargs: Any) -> None:
        self.columns = []
        self.names = []

        id_prefix = hex(id(self))[2:6]
        for i, column in enumerate(columns):
            self.columns.append(column)
            if isinstance(column, str):
                self.names.append(column)
                continue
            try:
                # series
                name = column.name
            except AttributeError:
                name = f'_tmp{id_prefix}_{i}'
                logger.warning(
                    'Temporary name used for a nesting column, use '
                    'keyword argument instead to specify the key as name.'
                )
            self.names.append(name)

        for key, val in kwargs.items():
            self.columns.append(val)
            self.names.append(key)

    def __len__(self):
        return len(self.columns)
