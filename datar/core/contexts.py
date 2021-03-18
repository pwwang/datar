from collections import defaultdict
from pipda import register_func
from pipda.context import (
    Context, ContextEval as ContextEvalPipda, ContextSelect
)

class ContextEval(ContextEvalPipda):

    geattr = ContextEvalPipda.getitem # make sure f.size gets f['size']

class ContextEvalWithUsedRefs(ContextEval):
    name = 'eval-with-refs'

    def __init__(self):
        self.used_refs = defaultdict(lambda: 0)

    def getattr(self, data, ref):
        self.used_refs[ref] += 1
        return super().getattr(data, ref)

    def getitem(self, data, ref):
        self.used_refs[ref] += 1
        return super().getitem(data, ref)

class ContextSelectSlice(ContextSelect):
    """Mark the context to interpret slice

    Whether turn f[:3] to first 3 columns or just the slice itself.
    """

register_func.default_context = ContextEval()
