import pytest

import numpy
from datar.all import *

from .conftest import assert_iterable_equal, assert_factor_equal

# fct_anon
def test_new_levels_are_padded_numerics():
    f1 = factor(letters[:10])
    f2 = fct_anon(f1)
    assert_iterable_equal(
        levels(f2), [str(i + 1).rjust(2, "0") for i in range(10)]
    )


def test_prefix_added_to_start_of_level():
    f1 = factor("x")
    f2 = fct_anon(f1, prefix="X")

    assert_iterable_equal(levels(f2), ["X1"])


# fct_collapse
def test_can_collapse_multiple_values():
    f1 = factor(letters[:3])
    f2 = fct_collapse(f1, x=c("a", "b"), y="c")
    f3 = factor(c("x", "x", "y"))
    assert_factor_equal(f2, f3)


def test_empty_dots_yields_unchanged_factor():
    f1 = factor(letters[1:3])
    f2 = fct_collapse(f1)

    assert_factor_equal(f1, f2)


def test_can_collapse_missing_levels():
    f1 = factor(c("x", "y"), exclude=NULL)
    f2 = fct_collapse(f1, y=["z"])

    assert_factor_equal(f2, factor(c("x", "y")))


def test_can_collapse_unnamed_levels_to_other():
    f1 = factor(letters[:3])
    f2 = fct_collapse(f1, xy=c("a", "b"), other_level="Other")

    assert_factor_equal(
        f2, factor(c("xy", "xy", "Other"), levels=c("xy", "Other"))
    )


def test_collapses_levels_correctly_when_group_other_is_TRUE_but_no_other_variables_to_group():
    f1 = factor(letters[:4])
    f2 = fct_collapse(f1, x1=c("a", "b", "d"), x2="c", other_level="Other")

    assert_factor_equal(
        f2, factor(c("x1", "x1", "x2", "x1"), levels=c("x1", "x2"))
    )


def test_collapses_levels_correctly_when_group_other_is_TRUE_and_some_Other_variables_to_group():
    f1 = factor(letters[:4])
    f2 = fct_collapse(f1, x1=c("a", "d"), x2="c", other_level="Other")

    assert_factor_equal(
        f2, factor(c("x1", "Other", "x2", "x1"), levels=c("x1", "x2", "Other"))
    )


def test_does_not_automatically_collapse_unnamed_levels_to_Other():
    f1 = factor(letters[:3])
    f2 = fct_collapse(f1, xy=c("a", "b"))

    assert_factor_equal(f2, factor(c("xy", "xy", "c"), levels=c("xy", "c")))


# fct_lump series


def test_too_many_arguments_fails():
    f = c("a", "b", "c")
    with pytest.raises(TypeError):
        fct_lump(f, n=1, count=1)
    with pytest.raises(ValueError):
        fct_lump(f, n=1, prop=0.1)
    with pytest.raises(TypeError):
        fct_lump(f, min=2, count=1)


def test_positive_values_keeps_most_commmon():
    f = c("a", "a", "a", "b", "b", "c", "d", "e", "f", "g")

    assert_iterable_equal(levels(fct_lump(f, n=1)), c("a", "Other"))
    assert_iterable_equal(levels(fct_lump(f, n=2)), c("a", "b", "Other"))

    assert_iterable_equal(levels(fct_lump(f, prop=0.25)), c("a", "Other"))
    assert_iterable_equal(levels(fct_lump(f, prop=0.15)), c("a", "b", "Other"))


def test_ties_are_respected():
    f = c("a", "a", "a", "b", "b", "b", "c", "d")
    assert_iterable_equal(levels(fct_lump(f, 1)), c("a", "b", "Other"))


def test_negative_values_drop_most_common():
    f = c("a", "a", "a", "a", "b", "b", "b", "b", "c", "d")
    assert_iterable_equal(levels(fct_lump(f, n=-1)), c("c", "d", "Other"))
    assert_iterable_equal(levels(fct_lump(f, prop=-0.2)), c("c", "d", "Other"))


def test_return_original_factor_when_all_element_satisfy_n_p_condition():
    f = c("a", "a", "a", "b", "b", "c", "d", "e", "f", "g")

    assert_iterable_equal(
        levels(fct_lump(f, n=4)), c("a", "b", "c", "d", "e", "f", "g")
    )
    assert_iterable_equal(
        levels(fct_lump(f, n=10)), c("a", "b", "c", "d", "e", "f", "g")
    )
    assert_iterable_equal(
        levels(fct_lump(f, n=-10)), c("a", "b", "c", "d", "e", "f", "g")
    )

    assert_iterable_equal(
        levels(fct_lump(f, prop=0.01)), c("a", "b", "c", "d", "e", "f", "g")
    )
    assert_iterable_equal(
        levels(fct_lump(f, prop=-1)), c("a", "b", "c", "d", "e", "f", "g")
    )


def test_different_behaviour_when_apply_tie_function():
    f = c("a", "a", "a", "b", "b", "c", "d", "e", "f", "g")

    assert_iterable_equal(
        levels(fct_lump(f, n=4, ties_method="min")),
        c("a", "b", "c", "d", "e", "f", "g"),
    )
    assert_iterable_equal(
        levels(fct_lump(f, n=4, ties_method="max")), c("a", "b", "Other")
    )

    # Rank of c, d, e, f, g is (3+4+5+6+7)/5 = 5
    assert_iterable_equal(
        levels(fct_lump(f, n=4, ties_method="average")), c("a", "b", "Other")
    )
    assert_iterable_equal(
        levels(fct_lump(f, n=5, ties_method="average")),
        c("a", "b", "c", "d", "e", "f", "g"),
    )

    assert_iterable_equal(
        levels(fct_lump(f, n=4, ties_method="first")),
        c("a", "b", "c", "d", "Other"),
    )

    # assert_iterable_equal(
    #     levels(fct_lump(f, n=4, ties_method="last")),
    #     c("a", "b", "f", "g", "Other"),
    # )


def test_NAs_included_in_total():
    f = factor(c("a", "a", "b", "c", rep(NA, 7)))

    o1 = fct_lump(f, prop=0.10)
    assert_iterable_equal(levels(o1), c("a", "Other"))

    o2 = fct_lump(f, w=rep(1, 11), prop=0.10)
    assert_iterable_equal(levels(o2), c("a", "Other"))


def test_bad_weights_generate_error_messages():
    with pytest.raises(TypeError):
        fct_lump(letters, w=letters)
    with pytest.raises(ValueError, match="must be the same length"):
        fct_lump(letters, w=range(10))
    with pytest.raises(ValueError, match="must be non-negative"):
        fct_lump(letters, w=rep(-1, 26))


def test_values_are_correctly_weighted():
    f = c("a", "a", "a", "b", "b", "c", "d", "e", "f", "g")
    w = c(0.2, 0.2, 0.6, 2, 2, 6, 4, 2, 2, 1)
    f2 = c(
        "a",
        rep("b", 4),
        rep("c", 6),
        rep("d", 4),
        rep("e", 2),
        rep("f", 2),
        "g",
    )

    assert_iterable_equal(levels(fct_lump(f, w=w)), levels(fct_lump(f2)))
    assert_iterable_equal(
        levels(fct_lump(f, n=1, w=w)), levels(fct_lump(f2, n=1))
    )
    assert_iterable_equal(
        levels(fct_lump(f, n=-2, w=w, ties_method="first")),
        levels(fct_lump(f2, n=-2, ties_method="first")),
    )
    assert_iterable_equal(
        levels(fct_lump(f, n=99, w=w)), levels(fct_lump(f2, n=99))
    )
    assert_iterable_equal(
        levels(fct_lump(f, prop=0.01, w=w)), levels(fct_lump(f2, prop=0.01))
    )
    assert_iterable_equal(
        levels(fct_lump(f, prop=-0.25, w=w, ties_method="max")),
        levels(fct_lump(f2, prop=-0.25, ties_method="max")),
    )


def test_do_not_change_the_label_when_no_lumping_occurs():
    f = c("a", "a", "a", "a", "b", "b", "b", "c", "c", "d")
    assert_iterable_equal(levels(fct_lump(f, n=3)), c("a", "b", "c", "d"))
    assert_iterable_equal(levels(fct_lump(f, prop=0.1)), c("a", "b", "c", "d"))


def test_only_have_one_small_other_level():
    f = c("a", "a", "a", "a", "b", "b", "b", "c", "c", "d")
    assert_iterable_equal(levels(fct_lump(f)), c("a", "b", "c", "Other"))

def test_fct_lump_min():
    f = c("a", "a", "a", "b", "b", "c", "d", "e", "f", "g")
    with pytest.raises(ValueError, match="positive number"):
        fct_lump_min(f, min=-3)

    f = c("a", "a")
    f1 = fct_lump_min(f, min=0)
    assert_factor_equal(f1, factor(c("a", "a")))


def test_fct_lump_min_works_when_not_weighted():
    f = c("a", "a", "a", "b", "b", "c", "d", "e", "f", "g")

    assert_iterable_equal(levels(fct_lump_min(f, min=3)), c("a", "Other"))
    assert_iterable_equal(levels(fct_lump_min(f, min=2)), c("a", "b", "Other"))


def test_fct_lump_min_works_when_weighted():
    f = c("a", "b", "c", "d", "e")
    w = c(0.2, 2, 6, 4, 1)

    assert_iterable_equal(levels(fct_lump_min(f, min=6, w=w)), c("c", "Other"))
    assert_iterable_equal(
        levels(fct_lump_min(f, min=1.5, w=w)), c("b", "c", "d", "Other")
    )


def test_throws_error_if_n_or_prop_is_not_numeric():
    f = c("a", "a", "a", "a", "b", "b", "b", "c", "c", "d")
    with pytest.raises(TypeError):
        fct_lump(f, n="2")
    with pytest.raises(TypeError):
        fct_lump(f, prop="2")


def test_fct_lump_prop_works_when_not_weighted():
    f = c("a", "a", "a", "b", "b", "c", "d", "e", "f", "g")

    assert_iterable_equal(levels(fct_lump_prop(f, prop=0.2)), c("a", "Other"))
    assert_iterable_equal(
        levels(fct_lump_prop(f, prop=0.1)), c("a", "b", "Other")
    )


def test_fct_lump_prop_works_when_weighted():
    f = c("a", "b", "c", "d", "e")
    w = c(0.2, 2, 6, 4, 1)

    assert_iterable_equal(
        levels(fct_lump_prop(f, prop=0.3, w=w)), c("c", "d", "Other")
    )
    assert_iterable_equal(
        levels(fct_lump_prop(f, prop=0.2, w=w)), c("c", "d", "Other")
    )


# Default -----------------------------------------------------------------


def lump_test(x):
    from datar.forcats.lvl_value import in_smallest
    return paste(
        if_else(in_smallest(numpy.array(x)), "X", letters[seq_along(x, base0_=True)]),
        collapse="",
    )


def test_lumps_smallest():
    assert_iterable_equal(lump_test(c(1, 2, 3, 6)), "Xbcd")
    assert_iterable_equal(lump_test(c(1, 2, 3, 7)), "XXXd")

    assert_iterable_equal(lump_test(c(1, 2, 3, 7, 13)), "XXXde")
    assert_iterable_equal(lump_test(c(1, 2, 3, 7, 14)), "XXXXe")


def test_doesnt_lump_if_none_small_enough():
    assert_iterable_equal(lump_test(c(2, 2, 4)), "abc")


def test_order_doesnt_matter():
    assert_iterable_equal(lump_test(c(2, 2, 5)), "XXc")
    assert_iterable_equal(lump_test(c(2, 5, 2)), "XbX")
    assert_iterable_equal(lump_test(c(5, 2, 2)), "aXX")


# fct_other
def test_keeps_levels_in_keep():
    x1 = factor(c("a", "b"))
    x2 = fct_other(x1, keep="a")

    assert_iterable_equal(levels(x2), c("a", "Other"))


def test_drops_levels_in_drop():
    x1 = factor(c("a", "b"))
    x2 = fct_other(x1, drop="a")

    # other always placed at end
    assert_iterable_equal(levels(x2), c("b", "Other"))


def test_must_supply_exactly_one_of_drop_and_keep():
    f = factor(c("a", "b"))

    with pytest.raises(ValueError, match="exactly one"):
        fct_other(f)
    with pytest.raises(ValueError, match="exactly one"):
        fct_other(f, keep="a", drop="a")


# fct_recode
def test_args_not_all_dicts():
    f1 = factor(c("a", "b"))
    with pytest.raises(ValueError, match="all mappings"):
        fct_recode(f1, "e")

def test_warns_about_unknown_levels(caplog):
    f1 = factor(c("a", "b"))
    f2 = fct_recode(f1, d="e")
    assert "Unknown levels" in caplog.text
    assert_iterable_equal(levels(f2), levels(f1))


def test_can_collapse_levels():
    f1 = factor(c("a1", "a2", "b1", "b2"))
    f2 = factor(c("a", "a", "b", "b"))

    assert_factor_equal(fct_recode(f1, a=["a1", "a2"], b=["b1", "b2"]), f2)


def test_can_recode_multiple_levels_to_NA():
    f1 = factor(c("a1", "empty", "a2", "b", "missing"))
    f2 = factor(c("a", NA, "a", "b", NA))

    assert_factor_equal(
        fct_recode(f1, {NULL: ["missing", "empty"], "a": ["a1", "a2"]}), f2
    )


def test_can_just_remove_levels():
    f1 = factor(c("a", "missing"))
    f2 = factor(c("a", NA))

    assert_factor_equal(fct_recode(f1, {NULL: "missing"}), f2)


# fct_relabel
def test_identity():
    f1 = factor(c("a", "b"))

    assert_factor_equal(fct_relabel(f1, identity), f1)


def test_error_if_not_function():
    f1 = factor("a")
    with pytest.raises(TypeError, match="callable"):
        fct_relabel(f1, 1)


def test_error_if_level_not_character():
    f1 = factor("a")
    with pytest.raises(TypeError, match="no len"):
        fct_relabel(f1, lambda x: 1)


def test_error_if_level_has_different_length():
    f1 = factor(letters)
    with pytest.raises(ValueError, match="26 new levels, got 25"):
        fct_relabel(f1, lambda x: x[:len(x)-1])


def test_total_collapse():
    f1 = factor(letters)
    new_levels = lambda x: rep("1", length(x))

    assert_factor_equal(
        fct_relabel(f1, new_levels), factor(new_levels(letters))
    )


def test_additional_arguments():
    f1 = factor(letters)

    assert_factor_equal(
        fct_relabel(f1, paste0, "."), factor(paste0(letters, "."))
    )


def test_formulas_are_coerced_to_functions():
    f1 = factor(letters)

    assert_factor_equal(
        fct_relabel(f1, lambda x: paste0(x, ".")), factor(paste0(letters, "."))
    )


def test_string_input_is_coerced_to_a_factor():
    assert_factor_equal(
        fct_relabel(LETTERS[:2], _fun=lambda x: x), factor(LETTERS[:2])
    )
