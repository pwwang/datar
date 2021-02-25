from collections import defaultdict
from plyrda.middlewares import Inverted
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

class ContextEvalWithDesc(ContextEval):

    def getattr(self, data, ref):
        if isinstance(ref, Inverted):
            return data[ref].transform(lambda x: x[::-1])
        return data[ref]

    getitem = getattr
