from pandas.core.series import Series
from pipda import register_operators, Operators
from .utils import series_expand, series_expandable
from .common import UnaryNeg

class PlyrdaOperators(Operators):

    def neg(self):
        return UnaryNeg(self.operand)

    def _expand_series(self, other):
        if (not series_expandable(self.data, other) and
                not series_expandable(self.data, self.operand)):
            return self.operand, other

        operand = self.operand
        if isinstance(operand, Series):
            operand = series_expand(operand, self.data)
        else:
            other = series_expand(other, self.data)
        return operand, other

    def lt_default(self, other):
        """Default behavior for X < Y"""
        operand, other = self._expand_series(other)
        return operand < other

    def le_default(self, other):
        """Default behavior for X <= Y"""
        operand, other = self._expand_series(other)
        return operand <= other

    def eq_default(self, other):
        """Default behavior for X == Y"""
        operand, other = self._expand_series(other)
        return operand == other

    def ne_default(self, other):
        """Default behavior for X != Y"""
        operand, other = self._expand_series(other)
        return operand != other

    def gt_default(self, other):
        """Default behavior for X > Y"""
        operand, other = self._expand_series(other)
        return operand > other

    def ge_default(self, other):
        """Default behavior for X >= Y"""
        operand, other = self._expand_series(other)
        return operand >= other

    invert = neg

register_operators(PlyrdaOperators)
