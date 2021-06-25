# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-context.R
import pytest

from pandas.testing import assert_frame_equal
from datar.all import *

def test_cur_group():
    df = tibble(g = 1, x = 1)
    gf = df >> group_by(f.g)

    out = df >> summarise(key=[cur_group()]) >> pull(f.key, to='list')
    assert len(out) == 1
    assert dim(out[0]) == (1,0)

    out = gf >> summarise(key=[cur_group()]) >> pull(f.key, to='list')
    assert len(out) == 1
    assert out[0].equals(tibble(g=1))


def test_cur_group_id():
    df = tibble(x = c("b", "a", "b"))
    gf = df >> group_by(f.x)

    out = gf >> summarise(id=cur_group_id())
    # group_by not sorted
    expect = tibble(x = c("a", "b"), id = [0,1])
    assert out.equals(expect)

    out = gf >> mutate(id=cur_group_id())
    expect = tibble(x=["b", "a","b"], id=[1,0,1])
    assert_frame_equal(out, expect)

def test_cur_data_all():
    df = tibble(x = c("b", "a", "b"), y = [1,2,3])
    gf = df >> group_by(f.x)

    out = df >> summarise(x=[cur_data()]) >> pull(f.x, to='list')
    assert out[0].equals(df)

    out = gf >> summarise(x=[cur_data()]) >> pull(f.x)
    assert out.values[0].values.flatten().tolist() == [2]
    assert out.values[1].values.flatten().tolist() == [1,3]

    out = gf >> summarise(x=[cur_data_all()]) >> pull(f.x)
    assert out.values[0].values.flatten().tolist() == ["a", 2]
    assert out.values[1].values.flatten().tolist() == ["b", 1, "b", 3]

def test_cur_group_rows():
    df = tibble(x = c("b", "a", "b"), y = [1,2,3])
    gf = df >> group_by(f.x)

    out = gf >> summarise(x=[cur_group_rows()]) >> pull()
    assert out.values.tolist() == [[1], [0,2]]
    # data frame
    out = df >> summarise(x=[cur_group_rows()]) >> pull()
    assert out.values.tolist() == [[0,1,2]]


def test_cur_data_all_sequentially():
    df = tibble(a=1)
    out = df >> mutate(x = ncol(cur_data()), y = ncol(cur_data()))
    expect = tibble(a=1, x=1, y=2)
    assert out.equals(expect)

    gf = tibble(a = 1, b = 2) >> group_by(f.a)
    out = gf >> mutate(x = ncol(cur_data_all()), y = ncol(cur_data_all()))
    expect = tibble(a = 1, b = 2, x = 2, y = 3)
    assert out.equals(expect)

def test_errors():
    # with pytest.raises(TypeError):
    #     n()
    with pytest.raises(ValueError):
        cur_data()
    with pytest.raises(ValueError):
        cur_data_all()
    with pytest.raises(ValueError):
        cur_group()
    with pytest.raises(ValueError):
        cur_group_id()
    with pytest.raises(ValueError):
        cur_group_rows()

def test_cur_column():
    df = tibble(x=1, y=2, z=3)
    out = df >> mutate(across(f[f.x:f.z], (lambda x, y: y), y=cur_column()))
    assert out.values.tolist() == [['x', 'y', 'z']]
