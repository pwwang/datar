from collections import defaultdict
from pipda.context import ContextEval

class ContextEvalWithUsedRefs(ContextEval):

    def __init__(self):
        self.used_refs = defaultdict(lambda: 0)

    def getattr(self, data, ref):
        self.used_refs[ref] += 1
        return super().getattr(data, ref)

    def getitem(self, data, ref):
        self.used_refs[ref] += 1
        return super().getitem(data, ref)
