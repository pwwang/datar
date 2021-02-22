from abc import ABC
from pipda import Symbolic
from .utils import expand_collections

X = Symbolic('X')

class UnaryOp(ABC):

    def __init__(self, operand):
        self.operand = operand

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.operand}>'

class Inverted(UnaryOp):
    ...

class UnaryPos(UnaryOp):
    ...

class UnaryInvert(UnaryOp):
    ...

class Collection(list):

    def __init__(self, args):
        super().__init__(expand_collections(args))

    def __neg__(self):
        return Inverted(self)

    def __pos__(self):
        return UnaryPos(self)

    def __invert__(self):
        return UnaryInvert(self)
