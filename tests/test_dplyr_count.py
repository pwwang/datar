# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-count-tally.r
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
