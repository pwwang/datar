"""Operators for datar"""
import operator
from collections.abc import Sequence
from typing import Any, Callable
from functools import partial

from pipda import register_operator, Operator
from pipda.operator import OPERATORS

from .backends.pandas import Series
from .backends.pandas.core.groupby import SeriesGroupBy, DataFrameGroupBy

from ..core.broadcast import broadcast2
from .collections import Collection, Inverted, Negated, Intersect


def _binop(opfunc, left, right, boolean=False):
    left, right, grouper, is_rowwise = broadcast2(left, right)
    if boolean:
        if isinstance(left, Series):
            left = left.fillna(False).astype(bool)
        else:
            left = Series(left).fillna(False).astype(bool).values

        if isinstance(right, Series):
            right = right.fillna(False).astype(bool)
        else:
            right = Series(right).fillna(False).astype(bool).values

    out = opfunc(left, right)
    if grouper:
        out = out.groupby(
            grouper,
            observed=all(g._observed for g in grouper.groupings),
            sort=grouper._sort,
            dropna=grouper.dropna,
        )
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
            out = op_func(operand.obj).groupby(
                operand.grouper,
                observed=operand.observed,
                sort=operand.sort,
                dropna=operand.dropna,
            )
            if getattr(operand, "is_rowwise", False):
                out.is_rowwise = True
            return out

        return op_func(operand)

    def _arithmetize2(self, left: Any, right: Any, op: str) -> Any:
        """Operator for paired operands"""
        if OPERATORS[op][1]:
            op_func = getattr(operator, op[1:])
            return _binop(op_func, right, left)

        op_func = getattr(operator, op)
        return _binop(op_func, left, right)

    def invert(self, operand: Any) -> Any:
        """Interpretation for ~x"""
        if isinstance(operand, (slice, Sequence)):
            return Inverted(operand)

        return self._arithmetize1(operand, "invert")

    def neg(self, operand: Any) -> Any:
        """Interpretation for -x"""
        if isinstance(operand, (slice, Sequence)):
            return Negated(operand)
        return self._arithmetize1(operand, "neg")

    def pos(self, operand: Any) -> Any:
        """Interpretation for -x"""
        return self._arithmetize1(operand, "pos")

    def and_(self, left: Any, right: Any) -> Any:
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

        return _binop(operator.and_, left, right, boolean=True)

    def or_(self, left: Any, right: Any) -> Any:
        """Mimic the | operator in R.

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

        return _binop(operator.or_, left, right, boolean=True)

    def rand_(self, left: Any, right: Any) -> Any:
        return self.and_(right, left)

    def ror_(self, left: Any, right: Any) -> Any:
        return self.or_(right, left)

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

    # def _find_op_func(self, opname: str) -> Callable:
    #     """Find the function for the operator"""
    #     full_op_name = f"_op_{opname}"
    #     if full_op_name in dir(self):
    #         return getattr(self, full_op_name)

    #     return partial(self._arithmetize2, op=opname)

    def __getattr__(self, name: str) -> Callable:
        return partial(self._arithmetize2, op=name)
