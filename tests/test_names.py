# https://github.com/r-lib/vctrs/blob/master/tests/testthat/test-names.R
import pytest
from typing import Iterable

import numpy as np
from string import ascii_letters

from datar.core.names import (
    NameNonUniqueError,
    repair_names,
)


@pytest.mark.parametrize(
    "names,expect",
    [
        ([1, 2, 3], ["1", "2", "3"]),
        (["", np.nan], ["", ""]),
        (["", np.nan], ["", ""]),
        (["", "", np.nan], ["", "", ""]),
        (repair_names(["", "", np.nan], repair="minimal"), ["", "", ""]),
    ],
)
def test_minimal(names, expect):
    assert repair_names(names, repair="minimal") == expect


@pytest.mark.parametrize(
    "names,expect",
    [
        ([np.nan, np.nan], ["__0", "__1"]),
        (["x", "x"], ["x__0", "x__1"]),
        (["x", "y"], ["x", "y"]),
        (["", "x", "y", "x"], ["__0", "x__1", "y", "x__3"]),
        ([""], ["__0"]),
        ([np.nan], ["__0"]),
        (
            ["__20", "a__33", "b", "", "a__2__34"],
            ["__0", "a__1", "b", "__3", "a__4"],
        ),
        (["a__1"], ["a"]),
        (["a__2", "a"], ["a__0", "a__1"]),
        (["a__3", "a", "a"], ["a__0", "a__1", "a__2"]),
        (["a__2", "a", "a"], ["a__0", "a__1", "a__2"]),
        (["a__2", "a__2", "a__2"], ["a__0", "a__1", "a__2"]),
        (
            ["__20", "a__1", "b", "", "a__2"],
            ["__0", "a__1", "b", "__3", "a__4"],
        ),
        (
            repair_names(["__20", "a__1", "b", "", "a__2"], repair="unique"),
            ["__0", "a__1", "b", "__3", "a__4"],
        ),
        (
            ["", "x", "", "y", "x", "_2", "__"],
            ["__0", "x__1", "__2", "y", "x__4", "__5", "__6"],
        ),
    ],
)
def test_unique(names, expect):
    assert repair_names(names, repair="unique") == expect


def test_unique_algebraic_y():
    x = ["__20", "a__1", "b", "", "a__2", "d"]
    y = ["", "a__3", "b", "__3", "e"]
    # fix names on each, catenate, fix the whole
    z1 = repair_names(
        repair_names(x, repair="unique") + repair_names(y, repair="unique"),
        repair="unique",
    )
    z2 = repair_names(repair_names(x, repair="unique") + y, repair="unique")
    z3 = repair_names(x + repair_names(y, repair="unique"), repair="unique")
    z4 = repair_names(x + y, repair="unique")
    assert z1 == z2 == z3 == z4


@pytest.mark.parametrize(
    "names,expect",
    [
        (list(ascii_letters), list(ascii_letters)),
        (
            [np.nan, "", "x", "x", "a1:", "_x_y}"],
            ["__0", "__1", "x__2", "x__3", "a1_", "_x_y_"],
        ),
        (
            repair_names(
                [np.nan, "", "x", "x", "a1:", "_x_y}"], repair="universal"
            ),
            ["__0", "__1", "x__2", "x__3", "a1_", "_x_y_"],
        ),
        (["a", "b", "a", "c", "b"], ["a__0", "b__1", "a__2", "c", "b__4"]),
        ([""], ["__0"]),
        ([np.nan], ["__0"]),
        (["__"], ["__0"]),
        (["_"], ["_"]),
        (["_", "_"], ["___0", "___1"]),
        (["", "_"], ["__0", "_"]),
        (["", "", "_"], ["__0", "__1", "_"]),
        (["_", "_", ""], ["___0", "___1", "__2"]),
        (["_", "", "_"], ["___0", "__1", "___2"]),
        (["", "_", ""], ["__0", "_", "__2"]),
        (["__6", "__1__2"], ["__0", "__1"]),
        (["if__2"], ["_if"]),
        (
            ["", "_", np.nan, "if__4", "if", "if__8", "for", "if){]1"],
            [
                "__0",
                "_",
                "__2",
                "_if__3",
                "_if__4",
                "_if__5",
                "_for",
                "if___1",
            ],
        ),
        (["a b", "b c"], ["a_b", "b_c"]),
        (
            ["", "_2", "_3", "__4", "___5", "____6", "_____7", "__"],
            ["__0", "__1", "__2", "__3", "___5", "____6", "_____7", "__7"],
        ),
        (
            repair_names(
                ["", "_2", "_3", "__4", "___5", "____6", "_____7", "__"],
                repair="unique",
            ),
            ["__0", "__1", "__2", "__3", "___5", "____6", "_____7", "__7"],
        ),
        (
            [7, 4, 3, 6, 5, 1, 2, 8],
            ["_7", "_4", "_3", "_6", "_5", "_1", "_2", "_8"],
        ),
        (
            repair_names([7, 4, 3, 6, 5, 1, 2, 8], repair="unique"),
            ["_7", "_4", "_3", "_6", "_5", "_1", "_2", "_8"],
        ),
    ],
)
def test_universal(names, expect):
    assert repair_names(names, repair="universal") == expect


def test_check_unique():
    with pytest.raises(NameNonUniqueError):
        repair_names([np.nan], repair="check_unique")
    with pytest.raises(NameNonUniqueError):
        repair_names([""], repair="check_unique")
    with pytest.raises(NameNonUniqueError):
        repair_names(["a", "a"], repair="check_unique")
    with pytest.raises(NameNonUniqueError):
        repair_names(["__1"], repair="check_unique")
    with pytest.raises(NameNonUniqueError):
        repair_names(["__"], repair="check_unique")
    assert repair_names(["a", "b"], repair="check_unique") == ["a", "b"]


def test_custom_repair():
    def replace(names: Iterable[str]):
        return ["a", "b", "c"]

    out = repair_names([1, 2, 3], repair=replace)
    assert out == ["a", "b", "c"]

    with pytest.raises(ValueError):
        repair_names([1, 2, 3], repair=1)

    out = repair_names(["a", "b", "c"], repair=str.upper)
    assert out == ["A", "B", "C"]

    out = repair_names(["a", "b", "c"], repair=["x", "y", "z"])
    assert out == ["x", "y", "z"]
