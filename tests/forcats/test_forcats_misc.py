import pytest

import numpy
from datar import get_versions
from datar.all import *
from datar.forcats import as_factor
from ..conftest import assert_iterable_equal, assert_factor_equal

# fct_as_factor
def test_equivalent_to_fct_inorder():
    x = c("a", "z", "g")
    assert_factor_equal(as_factor(x), fct_inorder(x))


def test_leaves_factors_as_is():
    f1 = factor(letters)
    f2 = as_factor(f1)

    assert_factor_equal(f1, f2)


# def test_logical_has_fixed_levels():
#     f = as_factor(["FALSE"])
#     assert_factor_equal(levels(f), factor(c("FALSE", "TRUE")))


def test_supports_NA_89():
    x = c("a", "z", "g", NA)
    assert_factor_equal(as_factor(x), fct_inorder(x))

# fct_count
def test_0_count_for_empty_levels():
    f = factor(levels = c("a", "b"))
    assert_iterable_equal(fct_count(f).n, c(0, 0))

    f = factor("a", levels = c("a", "b", "c"))
    assert_iterable_equal(fct_count(f).n, c(1, 0, 0))


def test_counts_NA_even_when_not_in_levels():
    f = factor(c("a", "a", NA))
    out = fct_count(f)

    assert_iterable_equal(out.n, c(2, 1))
    # and doesn't change levels
    # only for pandas 1.3+
    assert_iterable_equal(levels(out.f), levels(f))


def test_returns_marginal_table():
    f = factor(c("a", "a", "b"))
    out = fct_count(f, prop = TRUE)

    assert_iterable_equal(out.n, c(2, 1))
    assert_iterable_equal(out.p, c(2/3., 1/3.))


def test_sort_TRUE_brings_most_frequent_values_to_top():
    f = factor(c("a", "b", "b"))
    out = fct_count(f, sort = TRUE)

    assert_iterable_equal(out.f, factor(c("b", "a"), levels = c("a", "b")))

# fct_match
def test_equivalent_to_in_when_levels_present():
    f = factor(c("a", "b", "c", NA))
    assert_iterable_equal(fct_match(f, "a"), numpy.isin(f, ["a"]))
    assert_iterable_equal(fct_match(f, NA), numpy.isin(f, [NA]))


def test_error_when_levels_are_missing():
    f = factor(c("a", "b", "c"))
    with pytest.raises(ValueError, match="not present"):
        fct_match(f, "d")

# fct_unique
def test_fct_unique():
    f = factor(c("a", "b", "c", "a", "b", "c"))
    assert_factor_equal(fct_unique(f), factor(c("a", "b", "c")))

# lvls_reorder
def test_changes_levels_not_values():
    f1 = factor(c("a", "b"))
    f2 = factor(c("a", "b"), levels = c("b", "a"))

    assert_factor_equal(lvls_reorder(f1, f[2:1]), f2)


def test_idx_must_be_numeric():
    f = factor(c("a", "b"))
    with pytest.raises(ValueError, match="must be integers"):
        lvls_reorder(f, "a")


def test_must_have_one_integer_per_level():
    f = factor(c("a", "b", "c"))

    with pytest.raises(ValueError, match="one integer for each level"):
        lvls_reorder(f, c(1, 2))
    with pytest.raises(ValueError, match="one integer for each level"):
        lvls_reorder(f, c(1, 2, 2))
    with pytest.raises(ValueError, match="must be integers"):
        lvls_reorder(f, c(1, 2.5))


def test_can_change_ordered_status_of_output():
    f1 = factor(letters[:3])
    f2 = ordered(f1)

    assert not is_ordered(lvls_reorder(f1, f[:3]))
    assert not is_ordered(lvls_reorder(f1, f[:3], ordered = FALSE))
    assert is_ordered(lvls_reorder(f1, f[:3], ordered = TRUE))

    assert is_ordered(lvls_reorder(f2, f[:3]))
    assert not is_ordered(lvls_reorder(f2, f[:3], ordered = FALSE))
    assert is_ordered(lvls_reorder(f2, f[:3], ordered = TRUE))


# lvls_expand -------------------------------------------------------------

def test_changes_levels_not_values():
    f1 = factor(c("a"))
    f2 = factor(c("a"), levels = c("a", "b"))

    assert_factor_equal(lvls_expand(f1, c("a", "b")), f2)


def test_must_include_all_existing_levels():
    f1 = factor(c("a", "b"))
    with pytest.raises(ValueError, match="include all existing levels"):
        lvls_expand(f1, c("a", "c"))


# lvls_revalue ------------------------------------------------------------

def test_changes_values_and_levels():
    f1 = factor(c("a", "b"))
    f2 = factor(c("b", "a"), levels = c("b", "a"))

    assert_factor_equal(lvls_revalue(f1, c("b", "a")), f2)


def test_can_collapse_values():
    f1 = factor(c("a", "b"))
    f2 = factor(c("a", "a"))

    assert_factor_equal(lvls_revalue(f1, c("a", "a")), f2)


def test_preserves_missing_values():
    f1 = factor(c("a", NA), exclude = NULL)
    f2 = lvls_revalue(f1, levels(f1))
    assert_iterable_equal(levels(f2), levels(f1))


# def test_new_levels_must_be_a_character():
#     f1 = factor(c("a", "b"))
#     with pytest.raises(ValueError, match="character vector"):
#         lvls_revalue(f1, f[1:5])


def test_new_levels_must_be_same_length_as_existing_levels():
    f1 = factor(c("a", "b"))
    with pytest.raises(ValueError, match="same length"):
        lvls_revalue(f1, c("a"))
    with pytest.raises(ValueError, match="same length"):
        lvls_revalue(f1, c("a", "b", "c"))


def test_lvls_union():
    a = factor(["a", "b"])
    b = factor(["b", "c"])
    c = factor(["c", "d"])
    assert_iterable_equal(lvls_union([a,b,c]), ["a", "b", "c", "d"])
