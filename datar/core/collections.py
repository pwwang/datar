"""Provide Collection and related classes to mimic `c` from `r-base`"""

from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Optional, Union

import pandas

from .utils import get_option
from .types import is_iterable, is_scalar
from .exceptions import ColumnNotExistingError

PoolType = Optional[Union[Iterable, int]]
UNMATCHED = object()

class CollectionBase(ABC):
    """Abstract class for collections"""

    def __init__(
            self,
            *args: Any,
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        self.elems = args
        self.base0 = base0
        self.pool = pool
        self.unmatched = set()
        self.error = None
        try:
            self.expand(pool=pool, base0=self.base0)
        except (ValueError, ColumnNotExistingError) as exc:
            self.error = exc

    @abstractmethod
    def expand(
            self,
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        """Expand the collection"""

class Collection(CollectionBase, list):
    """Mimic the c function in R

    All elements will be flattened.
    This is also the entry point to adopt 1-baed and 0-based indexing, and
    convert them into 0-based finally

    The Inverted, Negated and slice objects will be expanded immediately. This
    means there is no chance to apply `_base0` that is received later on. So
    the original elements are stored in `self.elems` to wait for a second
    evaluation with the correct `_base0`.

    Args:
        *args: The elements
        base0: Whether the index is 0-based or not. Should be an integer
            indicating the range or a list, not a generator.
        pool: The pool used to expand slice
    """
    def _get_base0(self, base0: Optional[bool]) -> bool:
        """Get base0 if specified, otherwise self.base0"""
        if base0 is None:
            base0 = self.base0
        self.base0 = get_option('index.base.0', base0)
        return self.base0

    def _get_pool(self, pool: PoolType) -> PoolType:
        """Get pool if specified, otherwise self.pool"""
        if pool is not None:
            self.pool = pool
        return self.pool

    def expand(
            self,
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        """Expand the elements of this collection

        A element could be either
        - a string (a column name),
        - an integer (a literal number, or an index of the pool)
        - a Collection
        - a Negated object
        - an Inverted object
        - a slice object

        If `base0` is passed, then it is used, otherwise use `self.base0`
        """
        base0 = self._get_base0(base0)
        pool = self._get_pool(pool)
        self.unmatched.clear()
        self.error = None

        if pool is not None:
            elems = [
                elem for elem in self.elems
                if not is_scalar(elem) or not pandas.isnull(elem)
            ]
        else:
            elems = self.elems

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
                    base0=base0
                )
            )
            return self

        if any(inverts):
            raise ValueError(
                "Cannot mix Inverted and non-Inverted elements "
                "in a collection.."
            )

        expanded = []
        expanded_append = expanded.append
        expanded_extend = expanded.extend
        for elem in elems:
            if isinstance(elem, slice):
                elem = Slice(elem, pool=pool, base0=base0)

            if isinstance(elem, CollectionBase):
                expanded_extend(elem.expand(pool, base0))
                self.unmatched.update(elem.unmatched)
            elif is_scalar(elem):
                elem = self._index_from_pool(elem)
                if elem is not UNMATCHED:
                    expanded_append(elem)
            else: # iterable
                exp = Collection(*elem, pool=pool, base0=base0)
                self.unmatched.update(exp.unmatched)
                expanded_extend(exp)
        list.__init__(self, expanded)
        return self

    def _is_index(self, elem: Any) -> bool:
        """Check if an element is an index or not"""
        if self.pool is None or not isinstance(elem, int):
            return False
        if isinstance(self.pool, int):
            return True
        # iterable
        # If an empty pool is given, assuming integer elem is index
        # Otherwise if pool is a list of integers, then elem should match
        # the pool
        if len(self.pool) == 0 or not isinstance(self.pool[0], int):
            return True
        return False

    def _index_from_pool(self, elem: Any, base0: Optional[bool] = None) -> Any:
        """Try to pull the index of the element from the pool"""
        if self.pool is None:
            # Return the element itself if pool is not specified
            # Then element is supposed to be a literal
            return elem

        if base0 is None:
            base0 = self.base0

        if self._is_index(elem):
            # elem is treated as an index if it is not an element of the pool
            if not base0 and elem == 0:
                raise ValueError('Index 0 given for 1-based indexing.')

            pool = len(self.pool) if is_iterable(self.pool) else self.pool
            out = elem - int(not base0) if elem >= 0 else elem + pool

            pool = list(range(pool))
            try:
                pool[out]
            except IndexError:
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
        return Negated(self, pool=self.pool, base0=self.base0)

    def __invert__(self):
        return Inverted(self, pool=self.pool, base0=self.base0)


class Negated(Collection):
    """Negated collection, representing collections by `-c(...)` or `-f[...]`"""

    def __repr__(self) -> str:
        return f"Negated({self.elems})"

    def expand(
            self,
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        """Expand the object"""
        super().expand(pool, base0)
        # self is now 0-based indexes

        # pylint: disable=bad-reversed-sequence
        if pool is not None:
            elems = [
                self._index_from_pool(-elem-int(not base0), base0=True)
                for elem in reversed(self)
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
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        """Expand the object"""
        if pool is None:
            raise ValueError("Inverted object needs `pool` to expand.")

        super().expand(pool, base0) # 0-based indexes
        pool = range(pool) if isinstance(pool, int) else range(len(pool))
        # pylint: disable=unsupported-membership-test
        list.__init__(self, [elem for elem in pool if elem not in self])
        return self

class Intersect(Collection):
    """Intersect of two collections, designed for `&` operator"""
    def __init__( # pylint: disable=super-init-not-called
            self,
            *args: Any,
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        if len(args) != 2:
            raise ValueError("Intersect can only accept two collections.")

        self.elems = args
        self.base0 = base0
        self.pool = pool
        self.unmatched = set()
        self.error = None
        # don't expand.

    def __repr__(self) -> str:
        return f"Intersect({self.elems})"

    def expand(
            self,
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        """Expand the object"""
        left = Collection(self.elems[0], pool=pool, base0=base0)
        right = Collection(self.elems[1], pool=pool, base0=base0)
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
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        if len(args) != 1 or not isinstance(args[0], slice):
            raise ValueError("Slice should wrap one and only one slice object.")
        self.slc = args[0]
        super().__init__(*args, pool=pool, base0=base0)

    def __repr__(self) -> str:
        return f"Slice({self.elems})"

    def expand(
            self,
            pool: PoolType = None,
            base0: Optional[bool] = None
    ) -> None:
        base0 = self._get_base0(base0)
        pool = self._get_pool(pool)
        self.unmatched.clear()
        self.error = None

        if pool is None:
            expanded = self._expand_no_pool(base0)
        else:
            expanded = self._expand_pool(pool, base0)

        list.__init__(self, expanded)
        return self

    def _expand_no_pool(self, base0: bool) -> List[Any]:
        """Expand slice literally

        Without pool or length, `[:3]` will expand to `0,1,2`,
        and `[-3:]` `-3,-2,-1`. But with length 10, `[-3:]` will expand to
        `7,8,9`
        """
        start, stop, step = self.slc.start, self.slc.stop, self.slc.step
        # Without pool, we don't know how to interpret strings in slice
        if isinstance(start, str) or isinstance(stop, str):
            raise ValueError(
                '`pool` is required when start/stop of slice are not indexes.'
            )

        if start is None:
            start = int(not base0)
        if stop is None:
            stop = int(not base0)
        if step is None:
            step = 1 if stop >= start else -1

        out = []
        out_append = out.append
        i = start
        while (i < stop) if step > 0 else (i > stop):
            out_append(i)
            i += step
        if not base0: # include stop
            out_append(stop)
        return out

    def _expand_pool(self, pool: PoolType, base0: bool) -> List[Any]:
        """Slice with pool given; try to match the range with the elements
        in the pool"""
        start, stop, step = self.slc.start, self.slc.stop, self.slc.step
        base = int(not base0)

        if start is None:
            start = 0
        elif not isinstance(start, int):
            if isinstance(pool, int) or start not in pool:
                raise ColumnNotExistingError(
                    f'Column `{start}` does not exist.'
                )
            start = self._index_from_pool(start)
        else:
            start -= base

        len_pool = pool if isinstance(pool, int) else len(pool)
        if stop is None:
            stop = len_pool
        elif not isinstance(stop, int):
            if isinstance(pool, int) or stop not in pool:
                raise ColumnNotExistingError(
                    f'Column `{stop}` does not exist.'
                )
            stop = self._index_from_pool(stop) + 1
        # else:
        #     stop += base

        if step == 0:
            stop -= 1
            step = 1 if stop >= start else -1

        return list(range(*slice(start, stop, step).indices(len_pool)))
