# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-arrange.r

import pytest
from datar.all import *

def test_empty_returns_self():
    df = tibble(x=range(1,11), y=range(1,11))
    gf = df >> group_by(f.x)

    out = df >> arrange()
    assert out is df

    out = gf >> arrange()
    assert out is gf

def test_sort_empty_df():
    df = tibble()
    out = df >> arrange()
    assert out is df

def test_na_end():
    df = tibble(x=c(2,1,NA)) # NA makes it float
    out = df >> arrange(f.x) >> pull()
    assert out.fillna(0.0).eq([1.0,2.0,0.0]).all()

def test_errors():
    ... # todo name repair