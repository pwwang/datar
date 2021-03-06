from functools import partial
import operator

from typing import Any
from pandas.core.frame import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy, SeriesGroupBy
from pandas.core.groupby.groupby import GroupBy
from pandas.core.series import Series
from pipda import register_operator, Operator, Context
from pipda.context import ContextSelect

from .utils import align_value, objectize, list_intersect, list_union
from .middlewares import Collection, Inverted, Negated

@register_operator
class DatarOperator(Operator):

    def _arithmetize1(self, operand: Any, op: str) -> Any:
        op_func = getattr(operator, op)
        operand = align_value(operand, self.data)
        ret = op_func(operand)
        if isinstance(self.data, DataFrameGroupBy):
            return ret.groupby(self.data.grouper, dropna=False)
        return ret

    def _arithmetize2(self, left: Any, right: Any, op: str) -> Any:
        op_func = getattr(operator, op)
        left = align_value(left, self.data)
        right = align_value(right, self.data)
        ret = op_func(left, right)
        if isinstance(self.data, DataFrameGroupBy):
            return ret.groupby(self.data.grouper, dropna=False)
        return ret

    def invert(self, operand: Any) -> Any:
        if isinstance(operand, (slice, str, list)):
            return Inverted(operand, self.data, self.context).complements
        return self._arithmetize1(operand, 'invert')

    def neg(self, operand: Any) -> Any:
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

    def ne(self, left: Any, right: Any) -> bool:
        return not self.eq(left, right)

    def __getattr__(self, name: str) -> Any:
        if not hasattr(operator, name):
            raise AttributeError
        return partial(self._arithmetize2, op=name)
