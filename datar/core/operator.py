"""Operators for datar"""
from typing import Callable
from contextlib import contextmanager

from pipda import register_operator, Operator


@register_operator
class DatarOperator(Operator):
    """Operator class for datar"""

    backend = None

    @classmethod
    @contextmanager
    def with_backend(cls, backend: str):
        """Use a backend for the operator"""
        old_backend = cls.backend
        cls.backend = backend
        yield
        cls.backend = old_backend

    def __getattr__(self, name: str) -> Callable:
        from .plugin import plugin
        return lambda x, y=None: plugin.hooks.operate(
            name,
            x,
            y,
            __plugin=self.__class__.backend,
        )
