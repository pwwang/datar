import pytest

from datar2.base.testing import *
from ..conftest import assert_iterable_equal

def test_is_numeric():
    a = np.array([1,2,3])
    assert is_numeric(a)

    assert is_numeric(1)
    assert is_numeric([1,2,3])

def test_is_integer():
    a = np.array([1,2,3])
    assert is_integer(a)

    assert is_integer(1)

    a = np.array([1,2], dtype=float)
    assert not is_integer(a)

def test_is_atomic():
    assert is_atomic(1)
    assert not is_atomic([1])

def test_is_element():
    assert is_element(1, [1,2])
    assert not is_element(0, [1,2])
    assert_iterable_equal(is_element([0,1], [1,2]), [False, True])

def test_all_any():
    assert all(is_atomic(x) for x in [1,2,3])
    assert any(is_atomic(x) for x in [1,[2,3]])
