"""Operators for datar"""
import operator
from typing import Any, Callable, Sequence
from functools import partial

from pandas.core.series import Series
from pandas.core.groupby import SeriesGroupBy, DataFrameGroupBy
from pipda import register_operator, Operator

from ..core.broadcast import broadcast2
from .collections import Collection, Inverted, Negated, Intersect


def _binop(op, left, right, fill_false=False):
    left, right, grouper, is_rowwise = broadcast2(left, right)
    if fill_false:
        if isinstance(left, Series):
            left = left.fillna(False)
        else:
            left = Series(left).fillna(False).values

        if isinstance(right, Series):
            right = right.fillna(False)
        else:
            right = Series(right).fillna(False).values

    out = op(left, right)
    if grouper:
        out = out.groupby(grouper)
        if is_rowwise:
            out.is_rowwise = True
    return out


@register_operator
class DatarOperator(Operator):
    """Operator class for datar"""

    def _arithmetize1(self, operand: Any, op: str) -> Any:
        """Operator for single operand"""
        op_func = getattr(operator, op)
        if isinstance(operand, (DataFrameGroupBy, SeriesGroupBy)):
            out = op_func(operand.obj).groupby(operand.grouper)
            if getattr(operand, "is_rowwise", False):
                out.is_rowwise = True
            return out

        return op_func(operand)

    def _arithmetize2(self, left: Any, right: Any, op: str) -> Any:
        """Operator for paired operands"""
        op_func = getattr(operator, op)
        return _binop(op_func, left, right)

    def _op_invert(self, operand: Any) -> Any:
        """Interpretation for ~x"""
        if isinstance(operand, (slice, Sequence)):
            return Inverted(operand)
        return self._arithmetize1(operand, "invert")

    def _op_neg(self, operand: Any) -> Any:
        """Interpretation for -x"""
        if isinstance(operand, (slice, Sequence)):
            return Negated(operand)
        return self._arithmetize1(operand, "neg")

    def _op_pos(self, operand: Any) -> Any:
        """Interpretation for -x"""
        return self._arithmetize1(operand, "pos")

    def _op_and_(self, left: Any, right: Any) -> Any:
        """Mimic the & operator in R.

        This has to have Expression objects to be involved to work

        Args:
            left: Left operand
            right: Right operand

        Returns:
            The intersect of the columns
        """
        if isinstance(left, Sequence) or isinstance(right, Sequence):
            # induce an intersect with Collection
            return Intersect(left, right)

        return _binop(operator.and_, left, right, fill_false=True)

    def _op_or_(self, left: Any, right: Any) -> Any:
        """Mimic the & operator in R.

        This has to have Expression objects to be involved to work

        Args:
            left: Left operand
            right: Right operand

        Returns:
            The intersect of the columns
        """
        if isinstance(left, Sequence) or isinstance(right, Sequence):
            # or union?
            return Collection(left, right)

        return _binop(operator.or_, left, right, fill_false=True)

    # def _op_eq(
    #     self, left: Any, right: Any
    # ) -> Union[bool, Sequence[bool], SeriesGroupBy]:
    #     """Do left == right"""
    #     left, right = broadcast2(left, right)
    #     return left == right

    # def _op_ne(
    #     self, left: Any, right: Any
    # ) -> Union[bool, Sequence[bool], SeriesGroupBy]:
    #     """Interpret for left != right"""
    #     left, right = broadcast2(left, right)
    #     return left != right

    def _find_op_func(self, opname: str) -> Callable:
        """Find the function for the operator"""
        full_op_name = f"_op_{opname}"
        if full_op_name in dir(self):
            return getattr(self, full_op_name)

        return partial(self._arithmetize2, op=opname)
