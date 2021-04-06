# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-rowwise.r
import pytest
from pandas.core.groupby.generic import DataFrameGroupBy
from datar.all import *

def test_preserved_by_major_verbs():
    rf = rowwise(tibble(x=range(1,6), y=[5,4,3,2,1]), "x")

    out = arrange(rf, f.y)
    assert out.flags.rowwise == ['x']
    assert group_vars(out) == ['x']

    out = filter(rf, f.x < 3)
    assert out.flags.rowwise == ['x']
    assert group_vars(out) == ['x']

    out = mutate(rf, x=f.x+1)
    assert out.flags.rowwise == ['x']
    assert group_vars(out) == ['x']

    out = rename(rf, X=f.x)
    assert out.flags.rowwise == ['X']
    assert group_vars(out) == ['X']

    out = select(rf, f.x)
    assert out.flags.rowwise == ['x']
    assert group_vars(out) == ['x']

    out = slice(rf, c(1,1))
    assert out.flags.rowwise == ['x']
    assert group_vars(out) == ['x']

    out = summarise(rf, z=mean([f.x, f.y]))
    assert isinstance(out, DataFrameGroupBy)
    assert group_vars(out) == ['x']

def test_rowwise_preserved_by_assign_only():
    rf = rowwise(tibble(x=range(1,6), y=[5,4,3,2,1]), "x")
    rf['z'] = [5,4,3,2,1]

    assert rf.flags.rowwise == ['x']
    assert group_vars(rf) == ['x']

def test_shows_in_display(caplog):
    rf = rowwise(tibble(x = range(1,6)), "x")
    rf >> display()
    assert "# [DataFrame] Rowwise: ['x']" in caplog.text

def test_rowwise_captures_group_vars():
    df = group_by(tibble(g = [1,2], x = [1,2]), f.g)
    rw = rowwise(df)
    assert group_vars(rw) == ['g']

    with pytest.raises(ValueError):
        rowwise(df, f.x)

def test_can_rowwise():
    rf1 = rowwise(tibble(x = range(1,6), y = range(1,6)), "x")
    rf2 = rowwise(rf1, f.y)
    assert group_vars(rf2) == ['y']
