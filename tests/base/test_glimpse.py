import pytest

from datar.base.glimpse import Glimpse, formatter
from datar.all import (
    f,
    group_by,
    glimpse,
    tibble,
    nest,
)


def test_glimpse_str_df(capsys):
    df = tibble(x=f[:10], y=[str(i) for i in range(10)])
    glimpse(df)
    out = capsys.readouterr().out
    assert "Rows: 10" in out
    assert "Columns: 2" in out
    assert "0, 1, 2" in out


def test_glimpse_str_nest_df(capsys):
    df = tibble(x=f[:10], y=f[10:20]) >> nest(data=~f.x)
    glimpse(df)
    out = capsys.readouterr().out
    assert "Rows: 10" in out
    assert "Columns: 2" in out
    assert "<DF 1x1>, <DF 1x1>" in out


def test_glimpse_str_gf(capsys):
    df = tibble(x=f[:10], y=[str(i) for i in range(10)]) >> group_by(f.y)
    glimpse(df)
    assert "Groups: y [10]" in capsys.readouterr().out


def test_glimpse_html_df():
    df = tibble(x=f[:20], y=[str(i) for i in range(20)])
    g = Glimpse(df, 100, formatter)
    assert repr(g).startswith("<Glimpse: ")

    out = g._repr_html_()
    assert "<table>" in out
