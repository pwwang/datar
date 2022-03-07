"""Provides specific contexts for datar"""
from collections import defaultdict
from enum import Enum
from pipda.context import (
    ContextBase,
    ContextEval as ContextEvalPipda,
    ContextMixed,
    ContextPending,
    ContextSelect,
)
from pandas import DataFrame


class ContextEval(ContextEvalPipda):
    """Evaluation context"""

    # make it slow
    # def eval_symbolic(self, data):
    #     from .tibble import Tibble

    #     if isinstance(data, (TibbleGrouped, DataFrame)) and not isinstance(
    #         data, Tibble
    #     ):
    #         from ..tibble import as_tibble
    #         return as_tibble(data)

    #     return super().eval_symbolic(data)

    def getitem(self, parent, ref, level):
        """Interpret f[ref]"""
        if level == 1 and isinstance(ref, slice):
            # Allow f[1:3] to be interpreted as 1:3
            from .collections import Collection

            return Collection(ref)

        if isinstance(parent, DataFrame):
            return parent[ref]

        return super().getitem(parent, ref, level)

    def getattr(self, parent, ref, level):
        """Evaluate f.a"""
        if isinstance(parent, DataFrame):
            return parent[ref]

        return super().getattr(parent, ref, level)

    @property
    def ref(self) -> ContextBase:
        """Defines how `item` in `f[item]` is evaluated.

        This function should return a `ContextBase` object."""
        return self


class ContextEvalRefCounts(ContextEval):
    """Evaluation context with used references traced"""

    def __init__(self, meta=None):
        super().__init__(meta)
        self.used_refs = defaultdict(lambda: 0)

    def getitem(self, parent, ref, level):
        """Interpret f[ref]"""
        if level == 1 and isinstance(ref, str):
            self.used_refs[ref] += 1

        return super().getitem(parent, ref, level)

    def getattr(self, parent, ref, level):
        """Evaluate f.a"""
        if level == 1 and isinstance(ref, str):
            self.used_refs[ref] += 1

        return super().getattr(parent, ref, level)


class Context(Enum):
    """Context enumerator for types of contexts"""

    UNSET = None
    PENDING = ContextPending()
    SELECT = ContextSelect()
    EVAL = ContextEval()
    MIXED = ContextMixed()
