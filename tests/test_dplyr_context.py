# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-context.R
import pytest

from datar.all import *

def test_cur_group():
    df = tibble(g = [1,2], x = [1,2])
    gf = df >> group_by(f.g)

    with pytest.raises(ValueError):
        df >> summarise(key=[cur_group()])

    out = gf >> summarise(key=[cur_group()]) >> pull(f.key)
    expect = tibble(g=1)
    assert out.values[0].equals(expect)
    expect = tibble(g=2)
    assert out.values[1].equals(expect)

def test_cur_group_id():
    df = tibble(x = c("b", "a", "b"))
    gf = df >> group_by(f.x)

    out = gf >> summarise(id=cur_group_id())
    # group_by not sorted
    expect = tibble(x = c("a", "b"), id = [0,1])
    assert out.obj.equals(expect)

    out = gf >> mutate(id=cur_group_id())
    # note the order has changed
    expect = tibble(x=["a", "b","b"], id=[0,1,1])
    assert out.obj.equals(expect)

def test_cur_data_all():
    df = tibble(x = c("b", "a", "b"), y = [1,2,3])
    gf = df >> group_by(f.x)

    with pytest.raises(ValueError):
        df >> summarise(x=[cur_data()])

    # todo
    # out = gf >> summarise(x=[cur_data()]) >> pull(f.x)
    # assert out.values[0].values.flatten().tolist() == [2]
    # assert out.values[1].values.flatten().tolist() == [1,3]

    out = gf >> summarise(x=[cur_data_all()]) >> pull(f.x)
    assert out.values[0].values.flatten().tolist() == ["a", 2]
    assert out.values[1].values.flatten().tolist() == ["b", 1, "b", 3]

def test_cur_group_rows():
    df = tibble(x = c("b", "a", "b"), y = [1,2,3])
    gf = df >> group_by(f.x)

    out = gf >> summarise(x=[cur_group_rows()]) >> pull()
    assert out.values.tolist() == [1, [0,2]]

def test_cur_data_all_sequentially():
    df = tibble(a=1) >> group_by(f.a)
    out = df >> mutate(x = ncol(cur_data()), y = ncol(cur_data()))
    expect = tibble(a=1, x=1, y=2)
    assert out.obj.equals(expect)

    gf = tibble(a = 1, b = 2) >> group_by(f.a)
    out = gf >> mutate(x = ncol(cur_data_all()), y = ncol(cur_data_all()))
    expect = tibble(a = 1, b = 2, x = 2, y = 3)
    assert out.obj.equals(expect)

def test_errors():
    with pytest.raises(TypeError):
        n()
    with pytest.raises(TypeError):
        cur_data()
    with pytest.raises(TypeError):
        cur_data_all()
    with pytest.raises(TypeError):
        cur_group()
    with pytest.raises(TypeError):
        cur_group_id()
    with pytest.raises(TypeError):
        cur_group_rows()
