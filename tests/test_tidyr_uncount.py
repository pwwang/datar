# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-uncount.R
import pytest
from datar.all import *
from pandas.testing import assert_frame_equal

def test_symbols_weights_are_dropped_in_output():
    df = tibble(x=1, w=1)
    assert_frame_equal(uncount(df, f.w), tibble(x=1))

def test_can_request_to_preserve_symbols():
    df = tibble(x=1, w=1)
    assert_frame_equal(uncount(df, f.w, _remove=False), df)

def test_unique_identifiers_created_on_request():
    df = tibble(w=f[1:3])
    assert_frame_equal(
        uncount(df, f.w, _id="id"),
        tibble(id=c(1, f[1:2], f[1:3]))
    )

def test_expands_constants_and_expressions():
    df = tibble(x=1, w=2)
    out = uncount(df, 2)
    assert_frame_equal(out, df.iloc[[0,0], :].reset_index(drop=True))

    out = uncount(df, 1+1)
    assert_frame_equal(out, df.iloc[[0,0], :].reset_index(drop=True))

def test_works_with_groups():
    df = tibble(g=1, x=1, w=1) >> group_by(f.g)
    out = uncount(df, f.w)
    exp = df >> select(~f.w)
    assert_frame_equal(out, exp)

def test_must_evaluate_to_integer():
    df = tibble(x=1, w=.5)

    out = uncount(df, f.w)
    assert nrow(out) == 0

    df = tibble(x=1)
    with pytest.raises(ValueError, match="`weights` must evaluate to numerics"):
        uncount(df, "W")

def test_works_with_0_weights():
    df = tibble(x=f[1:2], w=[0,1])
    assert_frame_equal(uncount(df, f.w), tibble(x=2))

def test_errors_on_negative_weights():
    df = tibble(x=1, w=-1)
    with pytest.raises(ValueError, match="must be >= 0"):
        uncount(df, f.w)
