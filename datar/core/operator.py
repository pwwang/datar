"""Operators for datar"""
from typing import Callable

from pipda import register_operator, Operator


@register_operator
class DatarOperator(Operator):
    """Operator class for datar"""

    def __getattr__(self, name: str) -> Callable:
        from .plugin import plugin
        return lambda x, y=None: plugin.hooks.operate(name, x, y)
