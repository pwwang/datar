import pytest

import numpy as np
from datar2.base.logical import *
from ..conftest import assert_iterable_equal

def test_as_logical():
    assert as_logical(1) is True
    assert_iterable_equal(as_logical([2,0]), [True, False])

def test_is_logical():
    assert not is_logical(1)
    assert is_logical(True)
    assert is_logical(np.array([True, False]))

def test_is_true():
    assert is_true(True)
    assert not is_true([True, True])

def test_is_false():
    assert is_false(False)
    assert not is_false([False, False])
