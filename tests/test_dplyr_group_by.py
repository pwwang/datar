#https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-group-by.r

import pytest

from datar.all import *
from datar.datasets import mtcars, iris

@pytest.fixture
def df():
    return tibble(x = rep([1,2,3], each = 10), y = rep(range(1,7), each = 5))

def test_add(df):
    tbl = df >> group_by(f.x, f.y, _add=True)
    gvars = group_vars(tbl)
    assert gvars == ['x', 'y']

    tbl = df >> group_by(f.x, _add=True) >> group_by(f.y, _add=True)
    gvars = group_vars(tbl)
    assert gvars == ['x', 'y']

# def test_join_preserve_grouping(df):
#     g = df >> group_by(f.x)

#     tbl = g >> inner_join(g, by=['x', 'y'])
#     gvars = tbl >> group_vars()
#     assert gvars == ['x']

#     tbl = g >> left_join(g, by=['x', 'y'])
#     gvars = tbl >> group_vars()
#     assert gvars == ['x']

#     tbl = g >> semi_join(g, by=['x', 'y'])
#     gvars = tbl >> group_vars()
#     assert gvars == ['x']

#     tbl = g >> anti_join(g, by=['x', 'y'])
#     gvars = tbl >> group_vars()
#     assert gvars == ['x']

def test_tibble_lose_grouping(df):
    g = df >> group_by(f.x)
    tbl = tibble(g)
    with pytest.raises(NotImplementedError):
        group_vars(tbl)

# group by a string is also referring to the column

def test_mutate_does_not_loose_variables():
    df = tibble(a = rep([1,2,3,4], 2), b = rep([1,2,3,4], each = 2), x = runif(8))
    by_ab = df >> group_by(f.a, f.b)
    by_a = by_ab >> summarise(x=sum(f.x), _groups="drop_last")
    by_a_quantile = by_a >> group_by(quantile=ntile(f.x, 4))

    assert by_a_quantile.obj.columns.tolist() == ["a", "b", "x", "quantile"]

def test_orders_by_groups():
    df = tibble(a = sample(range(1,11), 3000, replace = TRUE)) >> group_by(f.a)
    out = df >> count()
    assert out.a.tolist() == list(range(1,11))

    df = tibble(a = sample(letters[:10], 3000, replace = TRUE)) >> group_by(f.a)
    out = df >> count()
    assert out.a.tolist() == letters[:10]

    df = tibble(a = sample(sqrt(range(1,11)), 3000, replace = TRUE)) >> group_by(f.a)
    out = df >> count()
    expect = list(sqrt(range(1,11)))
    assert out.a.tolist() == expect

def test_by_tuple_values():
    df = tibble(
        x=[1,2,3],
        y=[(1,2), (1,2,3), (1,2)]
    ) >> group_by(f.y)
    out = df >> count()
    assert out.y.tolist() == [(1,2), (1,2,3)]
    assert out.n.tolist() == [2, 1]

def test_select_add_group_vars():
    res = mtcars >> group_by(f.vs) >> select(f.mpg)
    assert res.obj.columns.tolist() == ['vs', 'mpg']

def test_one_group_for_NA():
    x = c(NA, NA, NA, range(10,0,-1), range(10,0,-1))
    w = c(20, 30, 40, range(1,11), range(1,11))
    n_dist = n_distinct(x)
    assert n_dist == 11

    res = tibble(x = x, w = w) >> group_by(f.x) >> summarise(n = n())
    rows = res >> nrow()
    assert rows == 11

def test_zero_row_dfs():
    df = tibble(a=1,b=1,g=1).loc[[], :]
    dfg = df >> group_by(f.g, _drop=False)
    d = dfg >> dim()
    assert d == (0, 3)

    x = dfg >> summarise(n=n())
    d = x >> dim()
    assert d == (0, 2)
    with pytest.raises(NotImplementedError):
        group_vars(x)

    x = dfg >> mutate(c = f.b+1)
    d = x >> dim()
    assert d == (0, 4)
    gvars = x >> group_vars()
    assert gvars == ['g']

    x = dfg >> filter(f.a==100)
    d = x >> dim()
    assert d == (0, 3)
    gvars = x >> group_vars()
    assert gvars == ['g']

    x = dfg >> arrange(f.a, f.g)
    d = x >> dim()
    assert d == (0, 3)
    gvars = x >> group_vars()
    assert gvars == ['g']

    x = dfg >> select(f.a)
    d = x >> dim()
    assert d == (0, 2)
    gvars = x >> group_vars()
    assert gvars == ['g']

def test_does_not_affect_input_data():
    df = tibble(x=1)
    dfg = df >> group_by(f.x)
    assert df.x.tolist() == [1]

def test_0_groups():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> mutate(y=mean(f.x), z=+mean(f.x), n=n())
    assert res.obj.columns.tolist() == ['x', 'y', 'z', 'n']
    rows = res >> nrow()
    assert rows == 0

def test_0_groups_filter():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> filter(f.x > 3)
    d1 = df >> dim()
    d2 = res >> dim()
    assert d1 == d2
    assert df.obj.columns.tolist()  == res.obj.columns.tolist()

def test_0_groups_select():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> select(f.x)
    d1 = df >> dim()
    d2 = res >> dim()
    assert d1 == d2
    assert df.obj.columns.tolist()  == res.obj.columns.tolist()

def test_0_groups_arrange():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> arrange(f.x)
    d1 = df >> dim()
    d2 = res >> dim()
    assert d1 == d2
    assert df.obj.columns.tolist()  == res.obj.columns.tolist()

def test_0_vars(df):
    with pytest.raises(ValueError):
        df >> group_by()

def test_drop():
    res = iris >> filter(f.Species == "setosa") >> group_by(
        f.Species, _drop = TRUE
    )
    out = res >> count() >> nrow()
    assert out == 1

def test_remember_drop_true():
    res = iris >>  group_by(
        f.Species,
        _drop=True
    )
    res2 = res >> filter(f.Sepal_Length > 5)
    assert group_by_drop_default(res2)
