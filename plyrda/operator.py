import operator

from typing import Any
from pandas.core.groupby.generic import DataFrameGroupBy
from pandas.core.groupby.groupby import GroupBy
from pipda import register_operator, Operator, Context
from pipda.context import ContextSelect

from .utils import align_value, arithmetize, list_intersect, list_union
from .middlewares import Collection, Inverted

@register_operator
class PlyrdaOperator(Operator):

    def invert(self, operand: Any) -> Any:
        if isinstance(operand, (slice, str, list)):
            return Inverted(operand, self.data).complements
        return operator.invert(operand)

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

        return operator.and_(left, right)

    def rand(self, right: Any, left: Any) -> Any:
        return self.and_(left, right)

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

        return operator.or_(left, right)

    def ror(self, right: Any, left: Any) -> Any:
        return self.or_(left, right)

    def eq(self, left: Any, right: Any) -> bool:
        left = align_value(left, self.data)
        right = align_value(right, self.data)
        return left == right

    def ne(self, left: Any, right: Any) -> bool:
        return not self.eq(left, right)

    def truediv(self, left: Any, right: Any) -> Any:
        left = align_value(left, self.data)
        right = align_value(right, self.data)
        return operator.truediv(left, right)
