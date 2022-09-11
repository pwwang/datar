"""Middlewares for datar"""
from typing import Any, Mapping, Tuple


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
