# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-case-when.R
import pytest

import numpy
from datar.all import *
from datar.datasets import mtcars

def test_match_values_in_order():
    x = numpy.array([1,2,3])
    out = case_when(
        x,
        x <= 1, 1,
        x <= 2, 2,
        x <= 3, 3
    )
    assert (out == x).all()

def test_unmatched_gets_missing():
    x = numpy.array([1,2,3])
    out = case_when(
        x,
        x <= 1, 1,
        x <= 2, 2
    )
    out[numpy.isnan(out)] = 100
    assert out.tolist() == [1,2,100]

def test_missing_can_be_replaced():
    x = numpy.array([1,2,3, NA])
    out = case_when(
        x,
        x <= 1, 1,
        x <= 2, 2,
        numpy.isnan(x), 0
    )
    out[numpy.isnan(out)] = 100
    assert out.tolist() == [1,2,100,0]

def test_na_condition():
    # case_when requires first argument as data
    x = numpy.array([1,2,3])
    with pytest.raises(IndexError):
        # NA cannot be index of ndarray
        case_when(
            x,
            [True, False, NA], [1,2,3],
            True, 4
        )

def test_scalar_conditions():
    x = numpy.array([NA,NA,NA])
    out = case_when(
        x,
        True, [1,2,3],
        False, [4,5,6]
    )
    assert out.tolist() == [1,2,3]

def test_use_inside_mutate():
    out = mtcars >> head(4) >> mutate(
        out=case_when(
            f.cyl==4, 1,
            f['am']==1, 2,
            True, 0
        )
    )
    assert out.out.tolist() == [2,2,1,0]

def test_errors():
    x = numpy.array([NA]*10)
    with pytest.raises(IndexError):
        # condition has to be the same length as data
        case_when(
            x,
            [True, False], [1,2,3],
            [False, True], [1,2]
        )
    with pytest.raises(TypeError):
        case_when()
    with pytest.raises(NotImplementedError):
        case_when("a")
    with pytest.raises(ValueError):
        case_when([], 1)
