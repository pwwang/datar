# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-if-else.R and
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-case-when.R
import pytest
import numpy
from datar.all import *
from datar.datasets import mtcars

def test_scalar_true_false_are_vectorized():
    x = c(TRUE, TRUE, FALSE, FALSE)
    out = if_else(x, 1, 2)
    assert list(out) == [1,1,2,2]

def test_vector_true_false_ok():
    x = numpy.array([-1, 0, 1])
    out = if_else(x<0, x, 0)
    assert list(out) == [-1, 0, 0]

    out = if_else(x>0, x, 0)
    assert list(out) == [0, 0, 1]

def test_missing_values_are_missing():
    out = if_else(c(TRUE, NA, FALSE), -1, 1)
    assert out[0] == -1.
    assert numpy.isnan(out[1])
    assert out[2] == 1.

def test_if_else_errors():
    # ok, numbers are able to be converted to booleans
    out = if_else(range(1,11), 1, 2)
    assert list(out) == [1.] * 10

    data = numpy.array([1,2,3])
    with pytest.raises(ValueError, match="length"):
        if_else(data < 2, [1,2], [1,2,3])
    with pytest.raises(ValueError, match="length"):
        if_else(data < 2, [1,2,3], [1,2])

# case_hwne ------------------

def test_matches_values_in_order():
    x = numpy.array([1,2,3])
    out = case_when(
        x <= 1, 1,
        x <= 2, 2,
        x <= 3, 3
    )
    assert list(out) == [1,2,3]

def test_unmatched_gets_missing_value():
    x = numpy.array([1,2,3])
    out = case_when(
        x <= 1, 1,
        x <= 2, 2
    )
    assert out[0] == 1.
    assert out[1] == 2.
    assert numpy.isnan(out[2])

def test_missing_values_can_be_replaced():
    x = numpy.array([1,2,3, NA])
    out = case_when(
        x <= 1, 1,
        x <= 2, 2,
        numpy.isnan(x), 0
    )
    assert out[0] == 1.
    assert out[1] == 2.
    assert numpy.isnan(out[2])
    assert out[3] == 0.

def test_na_conditions():
    out = case_when(
        [TRUE, FALSE, NA], [1,2,3],
        TRUE, 4
    )
    assert list(out) == [1,4,4]

def test_atomic_conditions():
    out = case_when(
        TRUE, [1,2,3],
        FALSE, [4,5,6]
    )
    assert list(out) == [1,2,3]

    out = case_when(
        NA, [1,2,3],
        TRUE, [4,5,6]
    )
    assert list(out) == [4,5,6]

def test_0len_conditions_and_values():
    out = case_when(
        TRUE, [],
        FALSE, []
    )
    assert list(out) == []

    out = case_when(
        [], 1,
        [], 2
    )
    assert list(out) == []

def test_inside_mutate():
    out = mtcars >> get(f[:4]) >> mutate(
        out = case_when(
            f.cyl == 4, 1,
            f['am'] == 1, 2,
            TRUE, 0
        )
    ) >> pull(to='list')
    assert out == [2,2,1,0]


def test_errors():
    x = numpy.array([NA]*10)
    with pytest.raises(ValueError):
        # condition has to be the same length as data
        case_when(
            x,
            [True, False], [1,2,3],
            [False, True], [1,2]
        )
    with pytest.raises(ValueError):
        case_when()
    with pytest.raises(ValueError):
        case_when("a")
    # ok
    case_when([], 1)
