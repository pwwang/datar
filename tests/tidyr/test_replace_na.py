# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-replace_na.R
import pytest
import numpy
from datar.all import *
from pandas.testing import assert_frame_equal

from ..conftest import assert_iterable_equal
# vector ------------------------------------------------------------------

def test_empty_call_does_nothing():
    x = c(1, NA)
    assert_iterable_equal(replace_na(x), x)

    x = numpy.array(x)
    assert_iterable_equal(replace_na(x), x)

def test_missing_values_are_replaced():
    x = c(1, NA)
    assert_iterable_equal(replace_na(x, 0), c(1,0))
    assert_iterable_equal(replace_na([], x, 0), c(1,0))

    x = numpy.array(x)
    assert_iterable_equal(replace_na(x, 0), c(1,0))
    assert_iterable_equal(replace_na([], x, 0), c(1,0))

# data frame -------------------------------------------------------------

def test_df_empty_call_does_nothing():
    df = tibble(x=c(1, NA))
    out = replace_na(df)
    assert_frame_equal(out, df)

def test_df_missing_values_are_replaced():
    df = tibble(x=c(1, NA))
    out = replace_na(df, {'x': 0})
    assert_iterable_equal(out.x, c(1,0))

def test_df_no_complain_about_non_existing_vars():
    df = tibble(a=c(1, NA))
    out = replace_na(df, {'a': 100, 'b':0})
    assert_frame_equal(out, tibble(a=c(1,100), _dtypes=float))

def test_df_can_replace_NULLs_in_list_column():
    df = tibble(x=[[1], NULL])
    # replace with list not supported yet
    rs = replace_na(df, {'x': 2})
    assert_frame_equal(rs, tibble(x=[[1], 2]))
