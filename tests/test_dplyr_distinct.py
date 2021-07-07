# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-distinct.R
from datar.core.grouped import DataFrameRowwise
import pytest
from datar.all import *

from datar.datasets import iris
from datar.core.exceptions import ColumnNotExistingError

def test_single_column():
    df = tibble(
        x = c(1, 1, 1, 1),
        y = c(1, 1, 2, 2),
        z = c(1, 2, 1, 2)
    )
    x = df >> distinct(f.x, _keep_all=False)
    assert all(x.x == unique(df.x))

    y = df >> distinct(f.y, _keep_all=False)
    assert all(y.y == unique(df.y))

def test_0_col_df():
    df = tibble(x=range(10)) >> select(~f.x)
    cols = df >> distinct() >> ncol()
    assert cols == 0

def test_novar_use_all():
    df = tibble(x=[1,1], y=[2,2])
    expect = tibble(x=1, y=2)
    out = df >> distinct()
    assert out.equals(expect)

def test_keeps_only_specified_cols():
    df = tibble(x = c(1, 1, 1), y = c(1, 1, 1))
    expect = tibble(x=1)
    out = df >> distinct(f.x)
    assert out.equals(expect)

def test_unless_keep_all_true():
    df = tibble(x=[1,1,1], y=[3,2,1])
    expect1 = tibble(x=1)
    out1 = df >> distinct(f.x)
    assert out1.equals(expect1)

    expect2 = tibble(x=1, y=3)
    out2 = df >> distinct(f.x, _keep_all=True)
    assert out2.equals(expect2)

def test_not_duplicating_cols():
    df = tibble(a=[1,2,3], b=[4,5,6])
    out = df >> distinct(f.a, f.a)
    assert out.columns.tolist() == ['a']

    out = df >> group_by(f.a) >> distinct(f.a)
    assert out.columns.tolist() == ['a']

def test_grouping_cols_always_included():
    df = tibble(g = c(1, 2), x = c(1, 2))
    out = df >> group_by(f.g) >> distinct(f.x)

    assert out.columns.tolist() == ['g', 'x']

def test_switch_groupby_distinct_equal():
    df = tibble(g = c(1, 2), x = c(1, 2))

    df1 = df >> distinct() >> group_by(f.g)
    df2 = df >> group_by(f.g) >> distinct()

    assert df1.equals(df2)

def test_mutate_internally():
    df = tibble(g = c(1, 2), x = c(1, 2))

    df1 = df >> distinct(aa=f.g*2)
    df2 = df >> mutate(aa=f.g*2) >> distinct(f.aa)

    assert df1.equals(df2)

def test_on_iter_type():
    df = tibble(
        a = c("1", "1", "2", "2", "3", "3"),
        b = [("A", )]
    )
    df2 = tibble(
        x=range(1,6),
        y=[(1,2,3), (2,3,4), (3,4,5), (4,5,6), (5,6,7)]
    )

    out = df >> distinct()
    expect = df >> slice([1,3,5])
    assert out.equals(expect)

    out2 = df2 >> distinct()
    assert out2.equals(df2)

def test_preserves_order():
    d = tibble(x=[1,2], y=[3,4])
    out = d >> distinct(f.y, f.x)
    assert out.columns.tolist() == ['x', 'y']

def test_on_na():
    df = tibble(col_a=[1, NA, NA]) #>> mutate(col_a=f.col_a+0.0)
    rows = df >> distinct() >> nrow()
    assert rows == 2

def test_auto_splicing():
    species = tibble(Species=iris.Species)

    df1 = iris >> distinct(f.Species)
    df2 = iris >> distinct(species)
    assert df1.equals(df2)

    df3 = iris >> distinct(across(f.Species))
    assert df1.equals(df3)

    df4 = iris >> mutate(across(starts_with("Sepal"), round)) >> distinct(
        f.Sepal_Length, f.Sepal_Width)
    df5 = iris >> distinct(across(starts_with("Sepal"), round))
    assert df4.equals(df5)

def test_preserves_grouping():
    gf = tibble(x = c(1, 1, 2, 2), y = f.x) >> group_by(f.x)
    out = gf >> distinct()
    gvars = group_vars(out)
    assert gvars == ['x']

    out = gf >> distinct(x=f.x+2)
    gvars = group_vars(out)
    assert gvars == ['x']

def test_errors():

    df = tibble(g = c(1, 2), x = c(1, 2))

    with pytest.raises(ColumnNotExistingError):
        df >> distinct(f.aa, f.x)
    with pytest.raises(ColumnNotExistingError):
        df >> distinct(f.aa, f.bb)
    with pytest.raises(ColumnNotExistingError):
        df >> distinct(y=f.a+1)

def test_rowwise_df():
    df = tibble(x=[1,1,1,2], y=[1,2,2,2])
    rf = df >> rowwise()
    out = distinct(rf)
    exp = distinct(df)
    assert out.equals(exp)
    assert isinstance(out, DataFrameRowwise)
