"""Operators for datar"""
from typing import Any, Optional
from functools import partial
import operator

import numpy
from pandas import Series
from pipda import register_operator, Operator
from pipda.context import ContextBase

from .utils import align_value
from .collections import Collection, Inverted, Negated, Intersect
from .types import BoolOrIter

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
        if isinstance(operand, (slice, str, list, tuple, Collection)):
            return Inverted(operand)
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
        if isinstance(left, list):
            # induce an intersect with Collection
            return Intersect(left, right)

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
        if isinstance(left, list):
            return Collection(left, right)
            # left = vars_select(self.data.columns, left)
            # right = vars_select(self.data.columns, right)
            # out = union(left, right)
            # return list(self.data.columns[out])
        return self._arithmetize2(left, right, 'or_')

    # def eq(self, left: Any, right: Any) -> BoolOrIter:
    #     """Check equality of left and right"""
    #     if is_categorical_dtype(left) and is_categorical_dtype(right):

    #     if isinstance(left, Series):
    #         left = left.values
    #     if isinstance(right, Series):
    #         right = right.values

    #     # try to keep the dimensions
    #     # make dim1 vector a scalar since series comparions require index
    #     # to be the same
    #     if (
    #             isinstance(left, (Categorical, numpy.ndarray)) and
    #             isinstance(right, (Categorical, numpy.ndarray)) and
    #             len(left) != len(right)
    #     ):
    #         if len(left) == 1:
    #             left = left[0]
    #         elif len(right) == 1:
    #             right = right[0]

    #     out = self._arithmetize2(left, right, 'eq')
    #     if isinstance(out, bool):
    #         return out
    #     # try keep NAs
    #     if isinstance(left, numpy.ndarray):
    #         out[pandas.isna(left)] = numpy.nan
    #     if isinstance(right, numpy.ndarray):
    #         out[pandas.isna(right)] = numpy.nan
    #     # note adding NAs converts dtype to float
    #     return out

    # pylint: disable=invalid-name
    def ne(self, left: Any, right: Any) -> BoolOrIter:
        """Interpret for left != right"""
        out = self.eq(left, right)
        if isinstance(out, (numpy.ndarray, Series)):
            neout = ~out
            # neout[pandas.isna(out)] = numpy.nan
            return neout
        return not out

    def __getattr__(self, name: str) -> Any:
        """Other operators"""
        if not hasattr(operator, name):
            raise AttributeError
        attr = partial(self._arithmetize2, op=name)
        attr.__qualname__ = self._arithmetize2.__qualname__
        return attr
