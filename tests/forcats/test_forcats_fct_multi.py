import pytest

from datar.all import *

from ..conftest import assert_iterable_equal, assert_factor_equal

# fct_c
def test_fct_c():
    fs = [factor("a"), factor("b")]
    fab = factor(c("a", "b"))

    assert_factor_equal(fct_c(*fs), fab)

    fct = fct_c()
    assert_factor_equal(fct, factor())

def test_all_inputs_must_be_factors():
    with pytest.raises(TypeError):
        fct_c(1)


def test_empty_input_yields_empty_factor():
    assert_factor_equal(fct_c(factor()), factor())


# fct_cross
def test_fct_cross():
    f = fct_cross()
    assert_factor_equal(f, factor())

def test_empty_input_returns_empty_factor():
    assert_factor_equal(fct_cross(factor()), factor())


def test_gives_correct_levels():
    fruit = as_factor(c("apple", "kiwi", "apple", "apple"))
    colour = as_factor(c("green", "green", "red", "green"))
    f2 = fct_cross(fruit, colour)

    assert_iterable_equal(
        sorted(levels(f2)), sorted(c("apple:green", "kiwi:green", "apple:red"))
    )


def test_recycle_inputs():
    assert len(fct_cross(["a"], c("a", "b", "c"), "d")) == 3
    with pytest.raises(ValueError):
        fct_cross(c("a", "b", "c"), c("a", "b"))


def test_keeps_empty_levels_when_requested():
    fruit = as_factor(c("apple", "kiwi", "apple", "apple"))
    colour = as_factor(c("green", "green", "red", "green"))
    f2 = fct_cross(fruit, colour, keep_empty=TRUE)

    assert_iterable_equal(
        sorted(levels(f2)),
        sorted(c("apple:green", "kiwi:green", "apple:red", "kiwi:red")),
    )


def test_order_of_levels_is_preserved():
    fruit = as_factor(c("apple", "kiwi", "apple", "apple"))
    colour = as_factor(c("green", "green", "red", "green"))

    fruit = fct_relevel(fruit, c("kiwi", "apple"))
    colour = fct_relevel(colour, c("red", "green"))

    f2 = fct_cross(fruit, colour)

    assert_iterable_equal(
        sorted(levels(f2)), sorted(c("kiwi:green", "apple:red", "apple:green"))
    )


def test_gives_NA_output_on_NA_input():
    fruit = as_factor(c("apple", "kiwi", "apple", "apple"))
    colour = as_factor(c("green", "green", "red", "green"))
    fruit[0] = NA
    f2 = fct_cross(fruit, colour)

    assert is_na(f2[0]).all()


def test_gives_NA_output_on_NA_input_when_keeping_empty_levels():
    fruit = as_factor(c("apple", "kiwi", "apple", "apple"))
    colour = as_factor(c("green", "green", "red", "green"))
    fruit[0] = NA
    f2 = fct_cross(fruit, colour, keep_empty=TRUE)
    assert is_na(f2[0]).all()


def test_can_combine_more_than_two_factors():
    fruit = as_factor(c("apple", "kiwi", "apple", "apple"))
    colour = as_factor(c("green", "green", "red", "green"))
    eaten = c("yes", "no", "yes", "no")

    f2 = fct_cross(fruit, colour, eaten)

    assert_iterable_equal(
        sorted(levels(f2)),
        sorted(
            c(
                "apple:green:no",
                "apple:green:yes",
                "apple:red:yes",
                "kiwi:green:no",
            )
        ),
    )
