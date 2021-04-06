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
from .utils import copy_flags
from .exceptions import ColumnNotExistingError

class ContextEval(ContextEvalPipda):
    name: ClassVar[str] = 'eval'

    def __init__(self):
        self.used_refs = defaultdict(lambda: 0)

    def getitem(self, data, ref):
        self.used_refs[ref] += 1
        if isinstance(data, DataFrame) and ref not in data:
            cols = [col for col in data.columns if col.startswith(f'{ref}$')]
            if not cols:
                raise ColumnNotExistingError(ref)
            ret = data.loc[: cols]
            ret.columns = [col[(len(ref)+1):] for col in cols]
            copy_flags(ret, data)
            return ret
        try:
            return super().getitem(data, ref)
        except KeyError as keyerr:
            raise ColumnNotExistingError(str(keyerr)) from None

    getattr = getitem # make sure f.size gets f['size']

class ContextSelectSlice(ContextSelect):
    """Mark the context to interpret slice

    Whether turn f[:3] to first 3 columns or just the slice itself.
    """
    name: ClassVar[str] = 'select-slice'

class Context(Enum):

    UNSET = None
    PENDING = ContextPending()
    SELECT = ContextSelect()
    EVAL = ContextEval()
    MIXED = ContextMixed()
