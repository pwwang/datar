import pytest

from datar.all import (
    c,
    f,
    group_by,
    glimpse,
    tibble,
    nest,
)


def test_glimpse_str_df():
    df = tibble(x=c[:10], y=[str(i) for i in range(10)])
    out = str(glimpse(df))
    assert "Rows: 10" in out
    assert "Columns: 2" in out
    assert "0, 1, 2" in out


def test_glimpse_str_nest_df():
    df = tibble(x=c[:10], y=c[10:20]) >> nest(data=~f.x)
    out = str(glimpse(df))
    assert "Rows: 10" in out
    assert "Columns: 2" in out
    assert "<DF 1x1>, <DF 1x1>" in out


def test_glimpse_str_gf():
    df = tibble(x=c[:10], y=[str(i) for i in range(10)]) >> group_by(f.y)
    out = repr(glimpse(df))
    assert "Groups: y [10]" in out


def test_glimpse_html_df():
    df = tibble(x=c[:20], y=[str(i) for i in range(20)])
    g = glimpse(df, 100)

    out = g._repr_html_()
    assert "<table>" in out
