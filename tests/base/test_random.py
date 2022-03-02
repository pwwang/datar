import pytest  # noqa
import random
import numpy
from datar2.base.random import set_seed


def test_standard_random_seed():
    set_seed(900)
    assert random.randint(1, 6) == 5
    assert random.randint(1, 6) == 2
    assert random.randint(1, 6) == 3


def test_numpy_random_seed():
    set_seed(900)
    assert numpy.random.randint(1, 6) == 2
    assert numpy.random.randint(1, 6) == 1
    assert numpy.random.randint(1, 6) == 5
