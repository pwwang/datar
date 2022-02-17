"""Operators for datar"""
from typing import Any, Sequence, Union
from functools import partial
import operator

from pandas.core.series import Series
from pandas.core.groupby import SeriesGroupBy
from pipda import register_operator, Operator

from .utils import broadcast
from .collections import Collection, Inverted, Negated, Intersect


@register_operator
class DatarOperator(Operator):
    """Operator class for datar"""

    def _arithmetize1(self, operand: Any, op: str) -> Any:
        """Operator for single operand"""
        op_func = getattr(operator, op)
        if isinstance(operand, SeriesGroupBy):
            return op_func(operand.obj).groupby(operand.grouper)
        return op_func(operand)

    def _arithmetize2(self, left: Any, right: Any, op: str) -> Any:
        """Operator for paired operands"""
        op_func = getattr(operator, op)
        left, right, grouper = broadcast(left, right)
        out = op_func(left, right)
        if grouper is not None:
            return out.groupby(grouper)
        return out

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
        if isinstance(left, Sequence):
            # induce an intersect with Collection
            return Intersect(left, right)

        left, right, grouper = broadcast(left, right)
        left = Series(left).fillna(False)
        right = Series(right).fillna(False)
        if grouper:
            return (left & right).groupby(grouper)
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
        if isinstance(left, Sequence):
            # or union?
            return Collection(left, right)

        left, right, grouper = broadcast(left, right)
        left = Series(left).fillna(False)
        right = Series(right).fillna(False)
        if grouper:
            return (left | right).groupby(grouper)
        return left | right

    def _op_eq(
        self, left: Any, right: Any
    ) -> Union[bool, Sequence[bool], SeriesGroupBy]:
        """Do left == right"""
        left, right, grouper = broadcast(left, right)
        out = left == right
        if grouper:
            return out.groupby(grouper)
        return out

    def _op_ne(
        self, left: Any, right: Any
    ) -> Union[bool, Sequence[bool], SeriesGroupBy]:
        """Interpret for left != right"""
        left, right, grouper = broadcast(left, right)
        out = left != right
        if grouper:
            return out.groupby(grouper)
        return out

    def __getattr__(self, name: str) -> Any:
        """Other operators"""
        if name.startswith("_op_"):
            attr = partial(self._arithmetize2, op=name[4:])
            attr.__qualname__ = name
            return attr
        return super().__getattr__(name)
