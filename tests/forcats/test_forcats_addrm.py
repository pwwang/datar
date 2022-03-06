import pytest  # noqa

from datar.all import *
from ..conftest import assert_iterable_equal, assert_factor_equal

# fct_expand
def test_fct_expand():
    f = factor(sample(letters[:3], 20, replace = TRUE))
    f1 = fct_expand(f, "d", "e", "f")
    assert_iterable_equal(levels(f1), letters[:6])

    f2 = fct_expand(f, letters[:6])
    assert_iterable_equal(levels(f2), letters[:6])

# fct_explicit_na
def test_factor_unchanged_if_no_missing_levels():
    f1 = factor(letters[:3])
    f2 = fct_explicit_na(f1)

    assert_factor_equal(f1, f2)


def test_converts_implicit_NA():
    f1 = factor(c("a", NA))
    f2 = fct_explicit_na(f1)

    assert_factor_equal(f2, fct_inorder(c("a", "(Missing)")))


def test_converts_explicit_NA():
    f1 = factor(c("a", NA), exclude = NULL)
    f2 = fct_explicit_na(f1)

    assert_factor_equal(f2, fct_inorder(c("a", "(Missing)")))



# fct_drop
def test_doesnt_add_NA_level():
    f1 = factor(c("a", NA), levels = c("a", "b"))
    f2 = fct_drop(f1)

    assert_iterable_equal(levels(f2), ["a"])


def test_can_optionally_restrict_levels_to_drop():
    f1 = factor("a", levels = c("a", "b", "c"))

    assert_iterable_equal(levels(fct_drop(f1, only = "b")), c("a", "c"))
    assert_iterable_equal(levels(fct_drop(f1, only = "a")), c("a", "b", "c"))


def test_preserves_ordered_class_and_attributes():
    f1 = ordered(letters[:2], letters[:3])
    # attr(f1, "x") = "test"

    f2 = fct_drop(f1)
    assert is_ordered(f2)
    assert_iterable_equal(levels(f2), letters[:2])
    # expect_s3_class(f2, "ordered")
    # expect_equal(attr(f2, "x"), attr(f1, "x"))

# fct_unify
def test_fct_unify():
    fs = [factor("a"), factor("b"), factor(c("a", "b"))]
    out = fct_unify(fs)

    assert_factor_equal(out[0], factor("a", levels=c("a", "b")))
    assert_factor_equal(out[1], factor("b", levels=c("a", "b")))
    assert_factor_equal(out[2], factor(c("a", "b"), levels=c("a", "b")))
