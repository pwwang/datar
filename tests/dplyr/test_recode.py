# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-recode.R
import pytest
import numpy
from pandas import Series
from datar.all import *
from datar.dplyr.recode import NA_integer_, NA_character_
from ..conftest import assert_iterable_equal


def assert_factor_equal(f1, f2):
    assert_iterable_equal(f1, f2)
    assert_iterable_equal(levels(f1), levels(f2))


def test_positional_substitution_works():
    out = recode([0, 1], "a", "b")
    assert_iterable_equal(out, ["a", "b"])


def test_names_override_positions():
    out = recode([1, 2], {2: "b", 1: "a"})
    assert_iterable_equal(out, ["a", "b"])


def test_named_substitution_works():
    x1 = letters[:3]
    x2 = factor(x1)

    out = recode(x1, a="apple", _default="<NA>")
    assert_iterable_equal(out, ["apple", "<NA>", "<NA>"])
    out = recode(x2, a="apple", _default="<NA>")
    assert_factor_equal(
        out, factor(["apple", "<NA>", "<NA>"])
    )


def test_missing_values_replaced_by_missing_argument():
    out = recode(c(0, NA), "a")
    assert_iterable_equal(out, ["a", NA])

    out = recode(c(0, NA), "a", _missing="b")
    assert_iterable_equal(out, ["a", "b"])

    out = recode(c(letters[:3], NA), _missing="A")
    assert_iterable_equal(out, list("abcA"))


def test_unmatched_value_replaced_by_default_argument(caplog):
    out = recode([0, 1], "a")
    assert_iterable_equal(out, ["a", NA])
    assert "Unreplaced values treated as NA" in caplog.text

    out = recode(c(0, 1), "a", _default="b")
    assert_iterable_equal(out, ["a", "b"])

    out = recode(letters[:3], _default="A")
    assert_iterable_equal(out, ["A"] * 3)


def test_missing_default_place_nicely_together():
    out = recode(c(0, 1, NA), "a", _default="b", _missing="c")
    assert_iterable_equal(out, list("abc"))


def test_can_give_name_x():
    assert recode("x", x="a") == ["a"]


def test_default_works_when_not_all_values_are_named():
    x = rep([0, 1, 2], 3)
    out = recode(x, {2: 10}, _default=x)
    assert_iterable_equal(out, rep(c(0, 1, 10), 3))


def test_default_is_aliased_to_x_when_missing_and_compatible():
    x = letters[:3]
    assert_iterable_equal(recode(x, a="A"), list("Abc"))

    n = [0, 1, 2]
    assert_iterable_equal(recode(n, {0: 10}), c(10, 1, 2))


def test_default_is_not_aliased_to_x_when_missing_and_not_compatible(caplog):
    x = letters[:3]
    assert_iterable_equal(recode(x, a=1), c(1, NA, NA))
    assert "Unreplaced values treated as NA" in caplog.text
    caplog.clear()

    n = [0, 1, 2]
    assert_iterable_equal(recode(n, {0: "a"}), c("a", NA, NA))
    assert "Unreplaced values treated as NA" in caplog.text


def test_conversion_of_unreplaced_values_to_na_gives_warning(caplog):
    recode([0, 1, 2], {0: "a"})
    assert "treated as NA" in caplog.text
    caplog.clear()

    recode_factor(letters[:3], b=1, c=2)
    assert "treated as NA" in caplog.text


def test_args_kwargs_works_correctly():
    # test_that(".dot argument works correctly (PR #2110)", {
    x1 = letters[:3]
    x2 = [1, 2, 3]
    x3 = factor(x1)

    out = recode(x1, a="apple", b="banana", _default=None)
    exp = recode(x1, _default=None, **{"a": "apple", "b": "banana"})
    assert_iterable_equal(out, exp)

    out = recode(x1, a="apple", b="banana", _default=None)
    exp = recode(x1, a="apple", _default=None, **{"b": "banana"})
    assert_iterable_equal(out, exp)

    out = recode(x2, **{"1": 4, "2": 5}, _default=NA_integer_)
    exp = recode(x2, _default=NA_integer_, **{"1": 4, "2": 5})
    assert_iterable_equal(out, exp)

    out = recode(x2, **{"1": 4, "2": 5}, _default=NA_integer_)
    exp = recode(x2, {"1": 4}, _default=NA_integer_, **{"2": 5})
    assert_iterable_equal(out, exp)

    out = recode(Series(x2), **{"1": 4, "2": 5}, _default=NA_integer_)
    exp = recode(x2, {"1": 4}, _default=NA_integer_, **{"2": 5})
    assert_iterable_equal(out, exp)

    out = recode_factor(x3, a="apple", b="banana", _default=NA_character_)
    exp = recode_factor(
        x3, _default=NA_character_, **{"a": "apple", "b": "banana"}
    )
    assert_iterable_equal(out, exp)

    out = recode_factor(
        Series(x3), a="apple", b="banana", _default=NA_character_
    )
    assert_iterable_equal(out, exp)


# factor ------------------------------------------------------------------
def test_default_works_with_factors():
    out = recode(factor(letters[:3]), a="A")
    assert_iterable_equal(out, factor(list("Abc")))


def test_can_recode_factor_to_double():
    fct = factor(letters[:3])
    assert_iterable_equal(recode(fct, a=1, b=2, c=3), c(1, 2, 3))
    assert_iterable_equal(recode(fct, a=1, b=2), c(1, 2, NA))
    assert_iterable_equal(recode(fct, a=1, b=2, _default=99), c(1, 2, 99))


def test_recode_factor_handles_missing_and_default_levels(caplog):
    x = c(1, 2, 3, NA)
    out = recode_factor(x, {1: "z", 2: "y"})
    exp = factor(c("z", "y", NA, NA), levels=c("z", "y"))
    assert_iterable_equal(out, exp)
    assert_iterable_equal(levels(out), levels(exp))

    out = recode_factor(x, {1: "z", 2: "y"}, _default="D")
    exp = factor(c("z", "y", "D", NA), levels=c("z", "y", "D"))
    assert_iterable_equal(out, exp)
    assert_iterable_equal(levels(out), levels(exp))

    out = recode_factor(x, {1: "z", 2: "y"}, _default="D", _missing="M")
    exp = factor(c("z", "y", "D", "M"), levels=c("z", "y", "D", "M"))
    assert_iterable_equal(out, exp)
    assert_iterable_equal(levels(out), levels(exp))


def test_recode_factor_handles_vector_default():
    expected = factor(list("azy"), levels=list("zya"))
    # expected = factor(list("azy"), levels=list("azy"))
    x1 = letters[:3]
    x2 = factor(x1)

    out = recode_factor(x1, b="z", c="y")
    assert_iterable_equal(out, expected)
    assert_iterable_equal(levels(out), levels(expected))

    out = recode_factor(x2, b="z", c="y")
    assert_iterable_equal(out, expected)
    assert_iterable_equal(levels(out), levels(expected))

    out = recode_factor(x1, b="z", c="y", _default=x1)
    assert_iterable_equal(out, expected)
    assert_iterable_equal(levels(out), levels(expected))

    out = recode_factor(x2, b="z", c="y", _default=x1)
    assert_iterable_equal(out, expected)
    assert_iterable_equal(levels(out), levels(expected))


def test_can_recode_factor_with_redundant_levels():
    # out = recode(factor(letters[:4]), d="c", b="a")
    # exp = factor(list("aacc"), levels=c("a", "c"))
    # assert_iterable_equal(out, exp)
    # assert_iterable_equal(levels(out), levels(exp))

    out = recode_factor(letters[:4], d="c", b="a")
    exp = factor(list("aacc"), levels=c("c", "a"))
    assert_iterable_equal(out, exp)
    assert_iterable_equal(levels(out), levels(exp))


# Errors --------------------------------------------
def test_errors():
    with pytest.raises(ValueError):
        recode(factor("a"), a=5, _missing=10)

    # expect_snapshot(error = TRUE, recode("a", b = 5, "c"))
    # expect_snapshot(error = TRUE, recode(factor("a"), b = 5, "c"))

    ## no replacement
    with pytest.raises(ValueError):
        recode(seq(1, 5))
    with pytest.raises(ValueError):
        recode("a")
    with pytest.raises(ValueError):
        recode(factor("a"))

    with pytest.raises(ValueError, match="must be"):
        recode(letters[:3], a=1, b="c")

    with pytest.raises(ValueError):
        recode(factor([1, 2, 3]), "a", "b", "c")

    # named for character
    with pytest.raises(ValueError, match="All values must be named"):
        recode(letters[:3], 1)

    # unnamed for numeric
    with pytest.raises(ValueError, match="All values must be unnamed"):
        recode([1, 2, 3], a=1)

    # default type mismatch
    with pytest.raises(ValueError, match="must be str"):
        recode(
            [1, 2, 3], "x", _default=numpy.array([1, "a", "b"], dtype=object)
        )

    # default length mismatch
    with pytest.raises(ValueError, match="must be length 3"):
        recode([1, 2, 3], "x", _default=numpy.array(["a", "b"]))

    with pytest.raises(ValueError, match="must be length 1"):
        recode([1], "x", _default=numpy.array(["a", "b"]))
