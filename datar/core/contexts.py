"""Provides specific contexts for datar"""
from collections import defaultdict
from enum import Enum
from typing import Any, ClassVar
from pandas.core.frame import DataFrame
from pipda.context import (
    ContextBase,
    ContextEval as ContextEvalPipda,
    ContextMixed,
    ContextPending,
    ContextSelect,
)
from .exceptions import ColumnNotExistingError


class ContextEval(ContextEvalPipda):
    """Evaluation context with used references traced"""

    name: ClassVar[str] = "eval"

    def __init__(self):
        self.used_refs = defaultdict(lambda: 0)

    # pylint: disable=arguments-differ
    def getitem(
        self, parent: Any, ref: Any, is_direct: bool = False, _attr=False
    ) -> Any:
        """Interpret f[ref]"""

        if is_direct and isinstance(ref, slice):
            # Allow f[1:3] to be interpreted as 1:3
            from .collections import Collection

            return Collection(ref)

        if is_direct:
            self.used_refs[ref] += 1
        if isinstance(parent, DataFrame):
            from .utils import df_getitem

            try:
                return df_getitem(parent, ref)
            except KeyError as keyerr:
                raise ColumnNotExistingError(str(keyerr)) from None

        return (
            super().getitem(parent, ref)
            if not _attr
            else super().getattr(parent, ref)
        )

    def getattr(self, parent: Any, ref: Any, is_direct: bool = False) -> Any:
        """Evaluate f.a"""
        return self.getitem(parent, ref, is_direct, _attr=True)

    @property
    def ref(self) -> ContextBase:
        """Defines how `item` in `f[item]` is evaluated.

        This function should return a `ContextBase` object."""
        return self


class Context(Enum):
    """Context enumerator for types of contexts"""

    UNSET = None
    PENDING = ContextPending()
    SELECT = ContextSelect()
    EVAL = ContextEval()
    MIXED = ContextMixed()
