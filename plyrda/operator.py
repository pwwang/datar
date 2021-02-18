from datetime import date
import operator

from typing import Any
from pandas.core.series import Series
from pipda import register_operator, Operator, Context

from .utils import list_intersect, list_union, series_expand, series_expandable
from .middlewares import UnaryNeg
from .exceptions import ColumnNotExistingError

@register_operator(context={
    'neg': Context.UNSET,
    'pos': Context.UNSET,
    'invert': Context.UNSET,
    'floordiv': Context.UNSET,
    'rfloordiv': Context.UNSET,
    'and_': Context.UNSET,
    'rand': Context.UNSET,
    'or_': Context.UNSET,
    'ror': Context.UNSET,
})
class PlyrdaOperator(Operator):

    def floordiv(self, start: Any, end: Any) -> Any:
        """Mimic the range operator in R.

        This has to have Expression objects to be involved to work
        Note that both `start` and `end` will be included in the results

        Args:
            start: The starting column of the range
                If `None`, will range from the first column
            end: The ending column of the range
                If `None`, will range to the last column

        Returns:
            The matched columns

        Raises:
            ColumnNonExistingError: When `start` or `end` not found in the data
        """
        if self.context == Context.NAME:
            columns = self.data.columns.to_list()
            if start is None:
                start = columns[0]
            if end is None:
                end = columns[-1]

            if isinstance(start, str) and start not in columns:
                raise ColumnNotExistingError(f"Column `{start}` does not exist.")
            if isinstance(end, str) and end not in columns:
                raise ColumnNotExistingError(f"Column `{end}` does not exist.")

            start_index = columns.index(start) if isinstance(start, str) else start
            end_index = columns.index(end) + 1 if isinstance(end, str) else end
            return columns[start_index:end_index]
        return operator.floordiv(start, end)

    def rfloordiv(self, end: Any, start: Any) -> Any:
        """Right version of floordiv

        This has to have Expression objects to be involved to work
        Note that both `start` and `end` will be included in the results

        Args:
            start: The starting column of the range
                If `None`, will range from the first column
            end: The ending column of the range
                If `None`, will range to the last column

        Returns:
            The matched columns

        Raises:
            ColumnNonExistingError: When `start` or `end` not found in the data
        """
        return self.floordiv(start, end)

    def neg(self, operand: Any) -> Any:
        if isinstance(operand, (tuple, list, set, str)):
            return UnaryNeg(operand, self.data)

        return operator.neg(operand)

    def and_(self, left: Any, right: Any) -> Any:
        """Mimic the & operator in R.

        This has to have Expression objects to be involved to work

        Args:
            left: Left operand
            right: Right operand

        Returns:
            The intersect of the columns
        """
        if self.context == Context.NAME:
            left = left.complements if isinstance(left, UnaryNeg) else left
            right = right.complements if isinstance(right, UnaryNeg) else right
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
        if self.context == Context.NAME:
            left = left.complements if isinstance(left, UnaryNeg) else left
            right = right.complements if isinstance(right, UnaryNeg) else right
            return list_union(left, right)

        return operator.or_(left, right)

    def ror(self, right: Any, left: Any) -> Any:
        return self.or_(left, right)

    def eq(self, left: Any, right: Any) -> bool:
        if series_expandable(left, self.data):
            left = series_expand(left, self.data)
        if series_expandable(right, self.data):
            right = series_expand(right, self.data)
        return left == right

    def ne(self, left: Any, right: Any) -> bool:
        return not self.eq(left, right)
