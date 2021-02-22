from pandas.core.series import Series
from pipda import register_operators, Operators
from .utils import series_expand, series_expandable
from .common import Inverted

class PlyrdaOperators(Operators):

    def neg(self):
        return Inverted(self.operand)

    def _expand_series(self, other, drop_index='other'):
        if (not series_expandable(self.data, other) and
                not series_expandable(self.data, self.operand)):
            operand =  self.operand
        else:
            operand = self.operand
            if isinstance(operand, Series) and isinstance(other, Series):
                # X.mass / mean(X.mass)
                operand = (series_expand(operand, self.data)
                        if operand.shape[0] < other.shape[0]
                        else operand)
                other = (series_expand(other, self.data)
                        if operand.shape[0] > other.shape[0]
                        else other)
            elif isinstance(operand, Series):
                operand = series_expand(operand, self.data)
            else:
                other = series_expand(other, self.data)
        if drop_index == 'other':
            try:
                other = other.reset_index(drop=True)
            except AttributeError:
                pass
        if drop_index == 'both':
            try:
                other = other.reset_index(drop=True)
            except AttributeError:
                pass
            try:
                operand = operand.reset_index(drop=True)
            except AttributeError:
                pass
        return operand, other

    def add_default(self, other):
        """Default behavior for X + Y"""
        operand, other = self._expand_series(other)
        return operand + other

    def sub_default(self, other):
        """Default behavior for X - Y"""
        operand, other = self._expand_series(other)
        return operand - other

    def mul_default(self, other):
        """Default behavior for X * Y"""
        operand, other = self._expand_series(other)
        return operand * other

    def matmul_default(self, other):
        """Default behavior for X @ Y"""
        operand, other = self._expand_series(other)
        return operand @ other

    def truediv_default(self, other):
        """Default behavior for X / Y"""
        operand, other = self._expand_series(other)
        return operand / other

    def floordiv_default(self, other):
        """Default behavior for X // Y"""
        operand, other = self._expand_series(other)
        return operand // other

    def mod_default(self, other):
        """Default behavior for X % Y"""
        operand, other = self._expand_series(other)
        return operand % other

    def lshift_default(self, other):
        """Default behavior for X << Y"""
        operand, other = self._expand_series(other)
        return operand << other

    def rshift_default(self, other):
        """Default behavior for X >> Y"""
        operand, other = self._expand_series(other)
        return operand >> other

    def and_default(self, other):
        """Default behavior for X & Y"""
        operand, other = self._expand_series(other)
        return operand & other

    def xor_default(self, other):
        """Default behavior for X ^ Y"""
        operand, other = self._expand_series(other)
        return operand ^ other

    def or_default(self, other):
        """Default behavior for X | Y"""
        operand, other = self._expand_series(other)
        return operand | other

    def pow_default(self, other):
        """Default behavior for X ** Y"""
        operand, other = self._expand_series(other)
        return operand ** other

    def lt_default(self, other):
        """Default behavior for X < Y"""
        operand, other = self._expand_series(other, 'both')
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
