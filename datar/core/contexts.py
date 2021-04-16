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

    def getitem(self, parent, ref):
        """Interpret f[ref]"""
        self.used_refs[ref] += 1
        if isinstance(parent, DataFrame) and ref not in parent:
            cols = [col for col in parent.columns if col.startswith(f'{ref}$')]
            if not cols:
                raise ColumnNotExistingError(ref)
            ret = parent.loc[:, cols]
            ret.columns = [col[(len(ref)+1):] for col in cols]
            return ret
        try:
            return super().getitem(parent, ref)
        except KeyError as keyerr:
            raise ColumnNotExistingError(str(keyerr)) from None

    getattr = getitem # make sure f.size gets f['size']

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
