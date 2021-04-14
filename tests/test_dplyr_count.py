# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-count-tally.r
from pandas.core.groupby import groupby
from datar.core.grouped import DataFrameGroupBy
from pandas.core.frame import DataFrame
import pytest

from datar.all import *

def test_informs_if_n_column_already_present_unless_overridden(caplog):
    df1 = tibble(n = c(1, 1, 2, 2, 2))
    out = df1 >> count(f.n)
    assert out.columns.tolist() == ['n', 'nn']
    assert 'already present' in caplog.text

    caplog.clear()
    out = df1 >> count(f.n, name='n')
    assert out.columns.tolist() == ['n']
    assert caplog.text == ''

    out = df1 >> count(f.n, name='nn')
    assert out.columns.tolist() == ['n', 'nn']
    assert caplog.text == ''

    df2 = tibble(n = c(1, 1, 2, 2, 2), nn = range(1,6))
    out = df2 >> count(f.n)
    assert out.columns.tolist() == ['n', 'nn']
    assert 'already present' in caplog.text

    out = df2 >> count(f.n, f.nn)
    assert out.columns.tolist() == ['n', 'nn', 'nnn']
    assert 'already present' in caplog.text

def test_name_must_be_string():
    df = tibble(x = c(1, 2))
    with pytest.raises(ValueError):
        df >> count(f.x, name=1)
    with pytest.raises(ValueError):
        df >> count(f.x, name=letters)

def test_drop():
    df = tibble(f = factor("b", levels = c("a", "b", "c")))
    out = df >> count(f.f)
    assert out.n.tolist() == [1]

    out = df >> count(f.f, _drop = False)
    # note the order
    assert out.n.tolist() == [0,1,0]

    out = count(group_by(df, f.f, _drop = FALSE))
    # print(out.obj)
    assert out.n.tolist() == [0,1,0]

def test_preserve_grouping():
    df = tibble(g = c(1, 2, 2, 2))
    exp = tibble(g = c(1, 2), n = c(1, 3))

    out = df >> count(f.g)
    assert out.equals(exp)

    df1 = df >> group_by(f.g) >> count()
    df2 = exp >> group_by(f.g)
    assert df1.equals(df2)
    assert group_vars(df1) == group_vars(df2)


def test_output_preserves_class_attributes_where_possible():
    df = tibble(g=c(1,2,2,2))
    df.attrs['my_attr'] = 1

    out = df >> count(f.g)
    assert isinstance(out, DataFrame)
    assert out.attrs['my_attr'] == 1

    out = df >> group_by(f.g) >> count()
    assert isinstance(out, DataFrameGroupBy)
    assert group_vars(out) == ['g']
    assert 'my_attr' not in out.attrs

def test_can_only_explicitly_chain_together_multiple_tallies():
    df = tibble(g=c(1,1,2,2), n=[1,2,3,4])
    out = df >> count(f.g, wt=f.n)
    exp = tibble(g=[1,2], n=[3,7])
    assert out.equals(exp)

    out = df >> count(f.g, wt=f.n) >> count(wt=f.n)
    exp = tibble(n=10)
    assert out.equals(exp)

    out = df >> count(f.n)
    exp = tibble(n=[1,2,3,4], nn=[1,1,1,1])
    assert out.equals(exp)

# tally -------------------------------------------------------------------

def test_tally_can_sort_output():
    gf = group_by(tibble(x = c(1, 1, 2, 2, 2)), f.x)
    out = tally(gf, sort = TRUE)
    exp = tibble(x = c(2, 1), n = c(3, 2))
    assert out.equals(exp)

def test_weighted_tally_drops_nas():
    df = tibble(x=c(1,1,NA))
    out = tally(df, f.x) >> pull(to='list')
    assert out == [2]

def test_tally_drops_last_group(caplog):
    df = tibble(x=1, y=2, z=3)
    res = df >> group_by(f.x, f.y) >> tally(wt=f.z)
    assert caplog.text == ''
    assert group_vars(res) == ['x']

# add_count ---------------------------------------------------------------
def test_output_preserves_grouping():
    df = tibble(g=c(1,2,2,2))
    exp = tibble(g=c(1,2,2,2), n=c(1,3,3,3))

    out = df >> add_count(f.g)
    assert out.equals(exp)

    out = df >> group_by(f.g) >> add_count()
    exp >>= group_by(f.g)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

# add_tally ---------------------------------------------------------------

def test_can_add_tallies_of_a_variable():
    df = tibble(a=c(2,1,1))
    out = df >> group_by(f.a) >> add_tally()
    exp = group_by(tibble(a=c(2,1,1), n=c(1,2,2)), f.a)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

def test_add_tally_can_be_given_a_weighting_variable():
    df = tibble(a=c(1,1,2,2,2), w=c(1,1,2,3,4))

    out = df >> group_by(f.a) >> add_tally(wt=f.w) >> pull(f.n, to='list')
    assert out == [2,2,9,9,9]

    out = df >> group_by(f.a) >> add_tally(wt = f.w+1) >> pull(f.n, to='list')
    assert out == [4,4,12,12,12]

def test_can_override_output_column():
    df = tibble(g=c(1,1,2,2,2), x=c(3,2,5,5,5))
    out = add_tally(df, name="xxx")
    assert out.columns.tolist() == ["g", "x", "xxx"]


