"""Operators for datar"""
from functools import partial
import operator

from typing import Any, Optional
from pipda import register_operator, Operator
from pipda.context import ContextBase

from .utils import align_value, list_intersect, list_union
from .middlewares import Inverted, Negated

@register_operator
class DatarOperator(Operator):
    """Operator class for datar"""
    def _arithmetize1(self, operand: Any, op: str) -> Any:
        """Operator for single operand"""
        op_func = getattr(operator, op)
        operand = align_value(operand, self.data)
        return op_func(operand)

    def _arithmetize2(self, left: Any, right: Any, op: str) -> Any:
        """Operator for paired operands"""
        op_func = getattr(operator, op)
        left = align_value(left, self.data)
        right = align_value(right, self.data)
        return op_func(left, right)

    def invert(self, operand: Any, _context: Optional[ContextBase]) -> Any:
        """Interpretation for ~x"""
        if isinstance(operand, (slice, str, list)):
            return Inverted(operand, self.data, _context).complements
        return self._arithmetize1(operand, 'invert')

    def neg(self, operand: Any) -> Any:
        """Interpretation for -x"""
        if isinstance(operand, (slice, list)):
            return Negated(operand)
        return self._arithmetize1(operand, 'neg')

    def and_(self, left: Any, right: Any) -> Any:
        """Mimic the & operator in R.

        This has to have Expression objects to be involved to work

        Args:
            left: Left operand
            right: Right operand

        Returns:
            The intersect of the columns
        """
        if isinstance(left, list) and isinstance(left[0], str):
            return list_intersect(left, right)
        return self._arithmetize2(left, right, 'and_')

    def or_(self, left: Any, right: Any) -> Any:
        """Mimic the & operator in R.

        This has to have Expression objects to be involved to work

        Args:
            left: Left operand
            right: Right operand

        Returns:
            The intersect of the columns
        """
        if isinstance(left, list) and isinstance(left[0], str):
            return list_union(left, right)
        return self._arithmetize2(left, right, 'or_')

    def ne(self, left: Any, right: Any) -> bool: # pylint: disable=invalid-name
        """Interpret for left != right"""
        return not self.eq(left, right)

    def __getattr__(self, name: str) -> Any:
        """Other operators"""
        if not hasattr(operator, name):
            raise AttributeError
        attr = partial(self._arithmetize2, op=name)
        attr.__qualname__ = self._arithmetize2.__qualname__
        return attr
