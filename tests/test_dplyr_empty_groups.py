# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-empty-groups.R
import pytest
from datar.all import *

@pytest.fixture
def df():
    return tibble(
        e = 1,
        f = factor(c(1, 1, 2, 2), levels = [1,2,3]),
        g = c(1, 1, 2, 2),
        x = c(1, 2, 1, 4)
    # group_by(..., _drop=False) only works for a
    # single categorical columns
    ) >> group_by(f.f, _drop = FALSE)

def test_filter_slice_keep_zero_len_groups(df):
    out = filter(df, f.f == 1)
    gsize = group_size(out)
    assert gsize == [2,0,0]

    out = slice(df, 0)
    gsize = group_size(out)
    assert gsize == [1,1,0]

def test_filter_slice_retain_zero_group_labels(df):
    # count loses _drop=False
    out = ungroup(count(filter(df, f.f==1)))
    expect = tibble(
        f=factor([1,2,3], levels=[1,2,3]),
        n=[2,0,0]
    )
    assert out.equals(expect)

    out = ungroup(count(slice(df, 0)))
    expect = tibble(
        f=factor([1,2,3], levels=[1,2,3]),
        n=[1,1,0]
    )
    assert out.equals(expect)

def test_mutate_keeps_zero_len_groups(df):
    gsize = group_size(mutate(df, z=2))
    assert gsize == [2,2,0]

def test_summarise_returns_a_row_for_zero_len_groups(df):
    summarised = df >> summarise(z=n())
    rows = summarised >> nrow()
    assert rows == 3

def test_arrange_keeps_zero_len_groups(df):
    gsize = group_size(arrange(df))
    assert gsize == [2,2,0]

    gsize = group_size(arrange(df, f.x))
    assert gsize == [2,2,0]

def test_bind_rows(df):
    # wait for bind_rows
    # gg = bind_rows(df, df)
    # gsize = group_size(gg)
    # assert gsize == [4,4,0]
    pass

def test_join_respect_zero_len_groups():
    df1 = tibble(
        f=factor([1,1,2,2], levels=[1,2,3]),
        x=[1,2,1,4]
    ) >> group_by(f.f)
    df2 = tibble(
        f=factor([2,2,3,3], levels=[1,2,3]),
        x=[1,2,3,4]
    ) >> group_by(f.f)

    gsize = group_size(left_join(df1, df2, by=f.f))
    assert gsize == [2,4]
    gsize = group_size(right_join(df1, df2, by=f.f))
    assert gsize == [4,2]
    gsize = group_size(full_join(df1, df2, by=f.f))
    assert gsize == [2,4,2]
    gsize = group_size(anti_join(df1, df2, by=f.f))
    assert gsize == [2]
    gsize = group_size(inner_join(df1, df2, by=f.f))
    assert gsize == [4]

    df1 = tibble(
        f=factor([1,1,2,2], levels=[1,2,3]),
        x=[1,2,1,4]
    ) >> group_by(f.f, _drop=False)
    df2 = tibble(
        f=factor([2,2,3,3], levels=[1,2,3]),
        x=[1,2,3,4]
    ) >> group_by(f.f, _drop=False)

    gsize = group_size(left_join(df1, df2, by=f.f))
    assert gsize == [2,4,0]
    gsize = group_size(right_join(df1, df2, by=f.f))
    assert gsize == [0,4,2]
    gsize = group_size(full_join(df1, df2, by=f.f))
    assert gsize == [2,4,2]
    gsize = group_size(anti_join(df1, df2, by=f.f))
    assert gsize == [2,0,0]
    gsize = group_size(inner_join(df1, df2, by=f.f))
    assert gsize == [0,4,0]

def test_n_groups_respect_zero_len_groups():
    df = tibble(x=factor([1,2,3], levels=[1,2,3,4])) >> group_by(f.x, _drop=False)
    assert n_groups(df) == 4

def test_summarise_respect_zero_len_groups():
    df = tibble(x=factor(rep([1,2,3], each=10), levels=[1,2,3,4]))

    out = df >> group_by(f.x, _drop=False) >> summarise(n=n())
    assert out.n.tolist() == [10,10,10,0]
