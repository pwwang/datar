from pipda import register_operators, Operators
from .common import UnaryNeg

class PlyrdaOperators(Operators):

    def neg(self):
        return UnaryNeg(self.operand)

    invert = neg

register_operators(PlyrdaOperators)
