# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-extract.R
import pytest
from pandas.testing import assert_frame_equal
from datar.all import *
from ..conftest import assert_iterable_equal


def test_default_returns_first_alpha_group():
    df = tibble(x=c("a.b", "a.d", "b.c"))
    out = df >> extract(f.x, "A")
    assert_iterable_equal(out.A, ["a", "a", "b"])

def test_can_match_multiple_groups():
    df = tibble(x=c("a.b", "a.d", "b.c"))
    out = df >> extract(f.x, ["A", "B"], r'(\w+)\.(\w+)')
    assert_iterable_equal(out.A, ['a', 'a', 'b'])
    assert_iterable_equal(out.B, ['b', 'd', 'c'])

def test_can_drop_group():
    df = tibble(x = c("a.b.e", "a.d.f", "b.c.g"))
    out = df >> extract(f.x, ["x", NA, "y"], r'([a-z])\.([a-z])\.([a-z])')
    assert_iterable_equal(out.columns, ['x', 'y'])
    assert_iterable_equal(out.y, ['e', 'f', 'g'])

def test_match_failures_give_NAs():
    df = tibble(x=c("a.b", "a"))
    out = df >> extract(f.x, "a", "(b)")
    assert_iterable_equal(out.a, ["b", NA])

def test_extract_keeps_characters_as_character():
    df = tibble(x="X-1")
    # cannot do convert=True, but specify the specific dtype
    out = extract(df, f.x, c("x", "y"), r'(.)-(.)', convert={'y': int})
    assert_frame_equal(out, tibble(x="X", y=1))

def test_can_combine_into_multiple_columns():
    df = tibble(x="abcd")
    out = extract(df, f.x, c('a', 'b', 'a', 'b'), r'(.)(.)(.)(.)')
    assert_frame_equal(out, tibble(a = "ac", b = "bd"))

def test_groups_are_preserved():
    df = tibble(g=1, x="X1") >> group_by(f.g)
    rs = df >> extract(f.x, ['x', 'y'], '(.)(.)')
    assert group_vars(rs) == group_vars(df)

def test_informative_error_message_if_wrong_number_of_groups():
    df = tibble(x="a")

    with pytest.raises(ValueError, match="should define 1 groups"):
        extract(df, f.x, "y", ".")

    with pytest.raises(ValueError, match="should define 2 groups"):
        extract(df, f.x, ["y", "z"], ".")

def test_invalid_into():
    df = tibble(x="a")

    with pytest.raises(ValueError, match="must be a string"):
        extract(df, f.x, 1)

def test_convert_to_single_type():
    df = tibble(x='1.2')
    out = extract(df, f.x, ['a', 'b'], r'(\d)\.(\d)', convert=int)
    assert_frame_equal(out, tibble(a=1, b=2))
