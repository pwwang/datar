import pytest

import numpy
from datar.all import *

from ..conftest import assert_iterable_equal

# fct_relevel
def test_warns_about_unknown_levels(caplog):
    f1 = factor(c("a", "b"))

    caplog.clear()
    f2 = fct_relevel(f1, "d")
    assert "Unknown levels" in caplog.text

    assert_iterable_equal(levels(f1), levels(f2))

def test_moves_supplied_levels_to_front():
    f1 = factor(c("a", "b", "c", "d"))

    f2 = fct_relevel(f1, "c", "b")
    assert_iterable_equal(levels(f2), c("c", "b", "a", "d"))

def test_can_moves_supplied_levels_to_end():
    f1 = factor(c("a", "b", "c", "d"))

    f2 = fct_relevel(f1, "a", "b", after = 1)
    f3 = fct_relevel(f1, "a", "b", after = -1)
    assert_iterable_equal(levels(f2), c("c", "d", "a", "b"))
    assert_iterable_equal(levels(f3), c("c", "d", "a", "b"))

def test_can_relevel_with_function():
    f1 = fct_rev(factor(c("a", "b")))
    f2a = fct_relevel(f1, rev)
    # f2b = fct_relevel(f1, ~ rev(.))

    assert_iterable_equal(levels(f2a), c("a", "b"))
    # assert_iterable_equal(levels(f2b), c("a", "b"))

# fct_reorder, fct_inorder, fct_infreq, fct_inseq

def test_reorder_unmatched_lens():
    f1 = factor(c("a", "b", "c", "d"))
    with pytest.raises(ValueError):
        fct_reorder(f1, [1])
    with pytest.raises(ValueError):
        fct_reorder2(f1, [1], [2])

def test_can_reorder_by_2d_summary():
    df = tribble(
        f.g,  f.x,
        "a", 3,
        "a", 3,
        "b", 2,
        "b", 2,
        "b", 1
    )

    f1 = fct_reorder(df.g, df.x)
    assert_iterable_equal(levels(f1), c("b", "a"))

    f2 = fct_reorder(df.g, df.x, _desc = TRUE)
    assert_iterable_equal(levels(f2), c("a", "b"))

def test_can_reorder_by_2d_summary():
    df = tribble(
        f.g, f.x, f.y,
        "a", 1, 10,
        "a", 2.1, 5, # ties order differ
        "b", 1, 5,
        "b", 2, 10
    )

    f1 = fct_reorder2(df.g, df.x, df.y)
    assert_iterable_equal(levels(f1), c("b", "a"))

    f2 = fct_reorder(df.g, df.x, _desc = TRUE)
    assert_iterable_equal(levels(f2), c("a", "b"))


def test_complains_if_summary_doesnt_return_single_value():
    fun1 = lambda x: c(1, 2)
    fun2 = lambda x, y: []

    with pytest.raises(ValueError, match="single value per group"):
        fct_reorder(["a"], 1, _fun=fun1)
    with pytest.raises(ValueError, match="single value per group"):
        fct_reorder2(["a"], 1, 2, _fun=fun2)

def test_fct_infreq_respects_missing_values():
    f = factor(c("a", "b", "b", NA, NA, NA), exclude = FALSE)
    # assert_iterable_equal(levels(fct_infreq(f)), c(NA, "b", "a"))
    # NA cannot be used as categories for pandas.Categorical
    assert_iterable_equal(levels(fct_infreq(f)), c("b", "a"))

def test_fct_inseq_sorts_in_numeric_order():
    x = c("1", "2", "3")
    f1 = fct_inseq(factor(x, levels = c("3", "1", "2")))
    f2 = factor(x, levels = c("1", "2", "3"))
    assert_iterable_equal(f1, f2)
    assert_iterable_equal(levels(f1), levels(f2))

    # non-numeric go to end
    x = c("1", "2", "3", "a")
    f3 = fct_inseq(factor(x, levels = c("a", "3", "1", "2")))
    f4 = factor(x, levels = c("1", "2", "3", "a"))
    assert_iterable_equal(f3, f4)
    assert_iterable_equal(levels(f3), levels(f4))


def test_fct_inseq_gives_error_for_non_numericlevels():
    f = factor(c("c", "a", "a", "b"))
    with pytest.raises(ValueError, match="At least one existing level must be coercible to numeric"):
        fct_inseq(f)

def test_fct_inorder():
    f = factor(c("c", "a", "a", "b"), c("a", "b", "c"))
    f1 = fct_inorder(f)
    f2 = factor(c("c", "a", "a", "b"), c("c", "a", "b"))
    assert_iterable_equal(f1, f2)
    assert_iterable_equal(levels(f1), levels(f2))

def test_first2():
    out = first2([4,3,1,4], numpy.array([1,2,3,4]))
    assert out == 3

def test_shuffle():
    f = factor(c("c", "a", "a", "b"))
    f2 = fct_shuffle(f)
    assert_iterable_equal(f, f2)
    assert_iterable_equal(sorted(levels(f)), sorted(levels(f2)))

def test_shift():
    f = factor(c("c", "a", "a", "b"))
    f2 = fct_shift(f)
    assert_iterable_equal(f, f2)
    assert_iterable_equal(levels(f2), ["b", "c", "a"])
