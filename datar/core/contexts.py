"""Provides specific contexts for datar"""
from collections import defaultdict
from enum import Enum
from typing import ClassVar
from pandas.core.frame import DataFrame
from pipda.context import (
    ContextEval as ContextEvalPipda,
    ContextMixed,
    ContextPending,
    ContextSelect
)
from .exceptions import ColumnNotExistingError

class ContextEval(ContextEvalPipda):
    """Evaluation context with used references traced"""
    name: ClassVar[str] = 'eval'

    def __init__(self):
        self.used_refs = defaultdict(lambda: 0)

    # pylint: disable=arguments-differ
    def getitem(self, parent, ref, _attr=False):
        """Interpret f[ref]"""
        if isinstance(ref, slice):
            # Allow f[1:3] to be interpreted as 1:3
            from .collections import Collection
            return Collection(ref)

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

    def getattr(self, parent, ref):
        """Evaluate f.a"""
        return self.getitem(parent, ref, _attr=True)

class ContextSelectSlice(ContextSelect):
    """Mark the context to interpret slice

    Whether turn f[:3] to first 3 columns or just the slice itself.
    """
    name: ClassVar[str] = 'select-slice'

class Context(Enum):
    """Context enumerator for types of contexts"""
    UNSET = None
    PENDING = ContextPending()
    SELECT = ContextSelect()
    EVAL = ContextEval()
    MIXED = ContextMixed()
