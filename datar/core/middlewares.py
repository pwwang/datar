"""Middlewares for datar"""
from typing import Any, Mapping, Tuple
from pipda.utils import DataEnv


class CurColumn:
    """Current column in across"""

    @classmethod
    def replace_args(cls, args: Tuple[Any], column: str) -> Tuple[Any, ...]:
        """Replace self with the real column in args"""
        return tuple(column if isinstance(arg, cls) else arg for arg in args)

    @classmethod
    def replace_kwargs(
        cls, kwargs: Mapping[str, Any], column: str
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
