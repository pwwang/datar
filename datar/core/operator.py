"""Operators for datar"""
from typing import Any, Tuple
from functools import partial
import operator

import numpy
from pandas import Series
from pipda import register_operator, Operator

from .utils import length_of, recycle_value
from .collections import Collection, Inverted, Negated, Intersect
from .exceptions import DataUnrecyclable
from .types import BoolOrIter

class DatarOperatorMeta(type):
    """Allow attributes with '_op_' to pass for operator functions"""
    def __getattr__(cls, name: str) -> Any:
        """If name starts with '_op_', let it go self for the real function
        Otherwise, do regular getattr.
        """
        if name.startswith('_op_'):
            return True
        return super().__getattr__(name)

@register_operator
class DatarOperator(Operator):
    """Operator class for datar"""

    def _arithmetize1(self, operand: Any, op: str) -> Any:
        """Operator for single operand"""
        op_func = getattr(operator, op)
        # Data length might be changed after evaluation
        # operand = recycle_value(operand, self.data.shape[0])
        return op_func(operand)

    def _arithmetize2(self, left: Any, right: Any, op: str) -> Any:
        """Operator for paired operands"""
        op_func = getattr(operator, op)
        left, right = _recycle_left_right(left, right)
        return op_func(left, right)

    def _op_invert(self, operand: Any) -> Any:
        """Interpretation for ~x"""
        if isinstance(operand, (slice, str, list, tuple)):
            return Inverted(operand)
        return self._arithmetize1(operand, "invert")

    def _op_neg(self, operand: Any) -> Any:
        """Interpretation for -x"""
        if isinstance(operand, (slice, list)):
            return Negated(operand)
        return self._arithmetize1(operand, "neg")

    def _op_and_(self, left: Any, right: Any) -> Any:
        """Mimic the & operator in R.

        This has to have Expression objects to be involved to work

        Args:
            left: Left operand
            right: Right operand

        Returns:
            The intersect of the columns
        """
        if isinstance(left, list):
            # induce an intersect with Collection
            return Intersect(left, right)

        left, right = _recycle_left_right(left, right)
        left = Series(left).fillna(False)
        right = Series(right).fillna(False)
        return left & right

    def _op_or_(self, left: Any, right: Any) -> Any:
        """Mimic the & operator in R.

        This has to have Expression objects to be involved to work

        Args:
            left: Left operand
            right: Right operand

        Returns:
            The intersect of the columns
        """
        if isinstance(left, list):
            return Collection(left, right)

        left, right = _recycle_left_right(left, right)
        left = Series(left).fillna(False)
        right = Series(right).fillna(False)
        return left | right

    # pylint: disable=invalid-name
    def _op_ne(self, left: Any, right: Any) -> BoolOrIter:
        """Interpret for left != right"""
        out = self._op_eq(left, right)
        if isinstance(out, (numpy.ndarray, Series)):
            neout = ~out
            # neout[pandas.isna(out)] = numpy.nan
            return neout
        # out is always a numpy.ndarray
        return not out  # pragma: no cover

    def __getattr__(self, name: str) -> Any:
        """Other operators"""
        if name.startswith('_op_'):
            attr = partial(self._arithmetize2, op=name[4:])
            attr.__qualname__ = self._arithmetize2.__qualname__
            return attr
        return super().__getattr__(name)


def _recycle_left_right(left: Any, right: Any) -> Tuple:
    """Recycle left right operands to each other"""
    try:
        left = recycle_value(left, length_of(right))
    except DataUnrecyclable:
        right = recycle_value(right, length_of(left))
    return left, right
