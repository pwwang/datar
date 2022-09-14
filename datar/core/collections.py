"""Provide Collection and related classes to mimic `c` from `r-base`"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, List, Mapping, Sequence, Union

import numpy as np
from pipda import evaluate_expr, Expression
from pipda.context import ContextType
from pipda.function import Function

from .backends import pandas as pd
from .backends.pandas.api.types import (
    is_array_like,
    is_scalar,
    is_integer,
    is_integer_dtype,
)

if TYPE_CHECKING:
    from pandas._typing import AnyArrayLike


UNMATCHED = object()


class CollectionBase(ABC):
    """Abstract class for collections"""

    def __init__(
        self,
        *args: Any,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> None:
        self.elems = args
        self.pool = pool
        self.unmatched = set()
        self.error = None
        try:
            self.expand(pool=pool)
        except (ValueError, KeyError) as exc:
            self.error = exc

    def _pipda_eval(self, data: Any, context: ContextType) -> Any:
        """Defines how the object should be evaluated when evaluated by
        pipda's evaluation"""
        self.elems = evaluate_expr(self.elems, data, context)
        return self

    @abstractmethod
    def expand(
        self,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> "CollectionBase":
        """Expand the collection"""


class Collection(CollectionBase, list):
    """Mimic the c function in R

    All elements will be flattened, indiced will be ignored.

    The Inverted, Negated and slice objects will be expanded immediately.

    Args:
        *args: The elements
        pool: The pool used to expand slice
    """

    def expand(
        self,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> CollectionBase:
        """Expand the elements of this collection

        A element could be either
        - a string (a column name),
        - an integer (a literal number, or an index of the pool)
        - a Collection
        - a Negated object
        - an Inverted object
        - a slice object
        """
        if pool is not None:
            self.pool = pool
        else:
            pool = self.pool

        self.unmatched.clear()
        self.error = None

        if pool is not None:
            elems = [
                elem
                for elem in self.elems
                if not is_scalar(elem) or not pd.isnull(elem)
            ]
        else:
            elems = self.elems  # type: ignore

        if not elems:
            list.__init__(self, [])
            return self

        inverts = [isinstance(elem, Inverted) for elem in elems]
        if all(inverts):
            list.__init__(
                self,
                Inverted(
                    Collection(*(elem.elems for elem in elems)),
                    pool=pool,
                ),
            )
            return self

        if any(inverts):
            raise ValueError(
                "Cannot mix Inverted and non-Inverted elements "
                "in a collection."
            )

        expanded = []
        expanded_append = expanded.append
        expanded_extend = expanded.extend
        for elem in elems:
            if isinstance(elem, slice):
                elem = Slice(elem, pool=pool)

            if isinstance(elem, CollectionBase):
                expanded_extend(elem.expand(pool))
                self.unmatched.update(elem.unmatched)
            elif is_scalar(elem) or isinstance(elem, Expression):
                elem = self._index_from_pool(elem)
                if elem is not UNMATCHED:
                    expanded_append(elem)
            else:  # iterable
                exp = Collection(*elem, pool=pool)
                self.unmatched.update(exp.unmatched)
                expanded_extend(exp)
        list.__init__(self, expanded)
        return self

    def _is_index(self, elem: Any) -> bool:
        """Check if an element is an index or not"""
        if is_integer(self.pool):
            return True

        if self.pool is None or not is_integer(elem):
            return False

        # iterable
        # If an empty pool is given, assuming integer elem is index
        # Otherwise if pool is a list of integers, then elem should match
        # the pool
        pool = np.array(self.pool)
        if len(pool) == 0 or not is_integer_dtype(pool):
            return True
        return False

    def _index_from_pool(self, elem: Any) -> Any:
        """Try to pull the index of the element from the pool"""
        if self.pool is None:
            # Return the element itself if pool is not specified
            # Then element is supposed to be a literal
            return elem

        if self._is_index(elem):
            # elem is treated as an index if it is not an element of the pool
            pool = (
                len(self.pool)
                if isinstance(self.pool, Sequence) or is_array_like(self.pool)
                else self.pool
            )
            out = elem if elem >= 0 else elem + pool

            # then the index should be 0 ~ len-1
            if not (0 <= out < pool):
                self.unmatched.add(elem)
                return UNMATCHED

            return out

        pool = range(self.pool) if is_scalar(self.pool) else self.pool

        if elem not in pool:
            self.unmatched.add(elem)
            return UNMATCHED

        return pool.index(elem)

    def __repr__(self) -> str:
        return f"Collection({self.elems})"

    def __str__(self) -> str:
        return list.__repr__(self)

    def __neg__(self):
        return Negated(self, pool=self.pool)

    def __invert__(self):
        return Inverted(self, pool=self.pool)


class Negated(Collection):
    """Negated collection, representing collections by `-c(...)` or `-f[...]`"""

    def __repr__(self) -> str:
        return f"Negated({self.elems})"

    def expand(
        self,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> None:
        """Expand the object"""
        super().expand(pool)
        # self is now 0-based indexes

        if self.pool is not None:
            elems = [
                self._index_from_pool(-elem if elem != 0 else -len(self))
                for elem in self
            ]
            # for elem in reversed(self):
            #     # If matched, it's sure an index.
            #     # Unmatched elements are stored in self.unmatched
            #     # if not self._is_index(elem):
            #     #     raise ValueError("Cannot negate a non-index value.")
            #     elems.append(-elem-1)
            list.__init__(self, elems)
        else:
            list.__init__(self, [-elem for elem in self])
        return self


class Inverted(Collection):
    """Inverted collection, tries to exlude some elements"""

    def __repr__(self) -> str:
        return f"Inverted({self.elems})"

    def expand(
        self,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> None:
        """Expand the object"""
        if pool is None:
            raise ValueError("Inverted object needs `pool` to expand.")

        super().expand(pool)  # 0-based indexes
        pool = range(pool) if is_integer(pool) else range(len(pool))

        list.__init__(self, [elem for elem in pool if elem not in self])
        return self


class Intersect(Collection):
    """Intersect of two collections, designed for `&` operator"""

    def __init__(
        self,
        *args: Any,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> None:
        if len(args) != 2:
            raise ValueError("Intersect can only accept two collections.")
        self.elems = args
        self.pool = pool
        self.unmatched = set()
        self.error = None
        # don't expand.

    def __repr__(self) -> str:
        return f"Intersect({self.elems})"

    def expand(
        self,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> None:
        """Expand the object"""
        left = Collection(self.elems[0], pool=pool)
        right = frozenset(Collection(self.elems[1], pool=pool))
        list.__init__(self, [elem for elem in left if elem in right])
        return self


class Slice(Collection):
    """Slice to wrap builtins.slice

    to sanitize slice objects, and compile it into a list of indexes

    If pool if given, then slice object will try to match the elements
    in pool.
    """

    def __init__(
        self,
        *args: Any,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> None:
        if len(args) != 1 or not isinstance(args[0], slice):
            raise ValueError(
                "Slice should wrap one and only one slice object."
            )
        self.slc = args[0]
        super().__init__(*args, pool=pool)

    def __repr__(self) -> str:
        return f"Slice({self.elems})"

    def expand(
        self,
        pool: Union[int, "AnyArrayLike"] = None,
    ) -> None:
        if pool is not None:
            self.pool = pool
        else:
            pool = self.pool

        self.unmatched.clear()
        self.error = None

        if pool is None:
            expanded = self._expand_no_pool()
        else:
            expanded = self._expand_pool(pool)

        list.__init__(self, expanded)
        return self

    def _expand_no_pool(self) -> List[Any]:
        """Expand slice literally

        Without pool or length, `[:3]` will expand to `0,1,2`,
        and `[-3:]` `-3,-2,-1`. But with length 10, `[-3:]` will expand to
        `7,8,9`
        """
        start, stop, step = self.slc.start, self.slc.stop, self.slc.step
        # Without pool, we don't know how to interpret strings in slice
        if isinstance(start, str) or isinstance(stop, str):
            raise ValueError(
                "`pool` is required when start/stop of slice are not indexes."
            )

        inclusive = step in (1, -1)

        if start is None:
            start = 0
        if stop is None:
            stop = 0
        if step is None:
            inclusive = False
            step = 1 if stop >= start else -1

        if inclusive:
            stop += step

        out = []
        out_append = out.append
        i = start
        while (i < stop) if step > 0 else (i > stop):
            out_append(i)
            i += step

        return out

    def _expand_pool(
        self,
        pool: Union[int, "AnyArrayLike"],
    ) -> List[Any]:
        """Slice with pool given; try to match the range with the elements
        in the pool"""
        start, stop, step = self.slc.start, self.slc.stop, self.slc.step

        if start is None:
            start = 0
        elif not is_integer(start):
            if is_integer(pool) or start not in pool:
                raise KeyError(start)
            start = self._index_from_pool(start)

        len_pool = pool if is_integer(pool) else len(pool)
        if stop is None:
            stop = len_pool
        elif not is_integer(stop):
            if is_integer(pool) or stop not in pool:
                raise KeyError(stop)
            stop = self._index_from_pool(stop)

        if step in (1, -1):
            if stop == 0 and step == -1:
                stop = None
            else:
                stop += step

        if step is None:
            step = 1 if stop >= start else -1

        return list(range(*slice(start, stop, step).indices(len_pool)))


class SubscrFunction(Function):

    """A function that is subscriptable"""

    def __getitem__(self, item: Any) -> CollectionBase:
        """Allow c[1:3] to be interpreted as 1:3"""
        return Collection(item)


def register_subscr_func(
    func: Callable = None,
    *,
    context: ContextType = None,
    extra_contexts: Mapping[str, ContextType] = None,
) -> SubscrFunction | Callable:
    """Register a function that is subscritable

    Args:
        func: The original function
        context: The context used to evaluate the arguments
        extra_contexts: Extra contexts to evaluate keyword arguments
            Note that the arguments should be defined as keyword-only arguments
            For example the argument `y` in `def fun(x, *, y): ...`

    Returns:
        A registered `SubscrFunction` object, or a decorator
        if `func` is not given
    """
    if func is None:  # pragma: no cover, only used once for base.c
        return lambda fun: register_subscr_func(
            fun,
            context=context,
            extra_contexts=extra_contexts or {},
        )

    return SubscrFunction(func, context, extra_contexts or {})
