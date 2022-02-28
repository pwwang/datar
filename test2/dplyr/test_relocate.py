# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-relocate.R
import pytest
from pandas.api.types import is_string_dtype, is_numeric_dtype
from datar2 import f
from datar2.testing import assert_tibble_equal
from datar2.tibble import tibble
from datar2.dplyr import relocate, where, last_col


def test_before_after_relocate_individual_cols():
    df = tibble(x=1, y=2)
    out = relocate(df, f.x, _after=f.y)
    assert out.columns.tolist() == ["y", "x"]

    out = relocate(df, f.y, _before=f.x)
    assert out.columns.tolist() == ["y", "x"]

    assert_tibble_equal(df, tibble(x=1, y=2))


def test_can_move_blocks_of_vars():
    df = tibble(x=1, a="a", y=2, b="a")
    out = df >> relocate(where(is_string_dtype))
    assert out.columns.tolist() == ["a", "b", "x", "y"]

    out = df >> relocate(where(is_string_dtype), _after=where(is_numeric_dtype))
    assert out.columns.tolist() == ["x", "y", "a", "b"]


def test_donot_lose_non_contiguous_vars():
    df = tibble(a=1, b=1, c=1, d=1, e=1)
    out = relocate(df, f.b, _after=[f.a, f.c, f.e])
    assert out.columns.tolist() == ["a", "c", "d", "e", "b"]

    out = relocate(df, f.e, _before=[f.b, f.d])
    assert out.columns.tolist() == ["a", "e", "b", "c", "d"]


def test_no_before_after_moves_to_front():
    df = tibble(x=1, y=2)
    out = relocate(df, f.y)
    assert out.columns.tolist() == ["y", "x"]


def test_can_only_supply_one_of_before_and_after():
    df = tibble(x=1)
    with pytest.raises(ValueError, match="only one"):
        relocate(df, _before=0, _after=0)


def test_respects_order_in_args_kwargs():
    df = tibble(a=1, x=1, b=1, z=1, y=1)

    out = relocate(df, f.x, f.y, f.z, _before=f.x)
    assert out.columns.tolist() == ["a", "x", "y", "z", "b"]

    out = relocate(df, f.x, f.y, f.z, _after=last_col())
    assert out.columns.tolist() == ["a", "b", "x", "y", "z"]

    out = relocate(df, f.x, f.a, f.z)
    assert out.columns.tolist() == ["x", "a", "z", "b", "y"]


def test_can_rename():
    df = tibble(a=1, b=1, c=1, d="a", e="a", f="a")

    out = relocate(df, ffff=f.f)
    assert out.equals(tibble(ffff="a", a=1, b=1, c=1, d="a", e="a"))

    out = relocate(df, ffff=f.f, _before=f.c)
    assert out.equals(tibble(a=1, b=1, ffff="a", c=1, d="a", e="a"))

    out = relocate(df, ffff=f.f, _after=f.c)
    assert out.equals(tibble(a=1, b=1, c=1, ffff="a", d="a", e="a"))


def test_before_0():
    df = tibble(x=1, y=2)
    out = relocate(df, f.y, _before=0)
    assert out.columns.tolist() == ["y", "x"]
