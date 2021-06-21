import pytest

from datar.base.which import *
from .conftest import assert_iterable_equal

def test_which():
    assert_iterable_equal(which([True, False, True]), [0,2])
    assert_iterable_equal(which([True, False, True], _base0=False), [1,3])

def test_which_min():
    assert which_min([2,1,3]) == 1

def test_which_max():
    assert which_max([2,1,3]) == 2
