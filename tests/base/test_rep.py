import pytest

from datar.core.backends.pandas.testing import assert_frame_equal
from datar import f
from datar.base import c, rep
from datar.core.tibble import TibbleGrouped
from datar.tibble import tibble

from ..conftest import assert_iterable_equal


@pytest.mark.parametrize(
    "x, times, length, each, expected",
    [
        (range(4), 2, None, 1, [0, 1, 2, 3] * 2),
        (range(4), 1, None, 2, [0, 0, 1, 1, 2, 2, 3, 3]),
        (range(4), [2] * 4, None, 1, [0, 0, 1, 1, 2, 2, 3, 3]),
        (range(4), [2, 1] * 2, None, 1, [0, 0, 1, 2, 2, 3]),
        (range(4), 1, 4, 2, [0, 0, 1, 1]),
        (range(4), 1, 10, 2, [0, 0, 1, 1, 2, 2, 3, 3, 0, 0]),
        (range(4), 3, None, 2, [0, 0, 1, 1, 2, 2, 3, 3] * 3),
        (1, 7, None, 1, [1, 1, 1, 1, 1, 1, 1]),
    ],
)
def test_rep(x, times, length, each, expected):
    assert_iterable_equal(
        rep(x, times=times, length=length, each=each), expected
    )


def test_rep_sgb_param(caplog):
    df = tibble(
        x=[1, 1, 2, 2],
        times=[1, 2, 1, 2],
        length=[3, 4, 4, 3],
        each=[1, 1, 1, 1],
    ).group_by("x")
    out = rep([1, 2], df.times)
    assert_iterable_equal(out.obj, [1, 2, 2, 1, 2, 2])

    out = rep([1, 2], times=df.times, each=1, length=df.length)
    assert "first element" in caplog.text

    assert_iterable_equal(out.obj, [1, 2, 2, 1, 2, 2, 1])
    assert_iterable_equal(out.grouper.size(), [3, 4])

    df2 = tibble(x=[1, 2], each=[1, 1]).group_by("x")
    out = rep(df2.x, each=df2.each)
    assert_iterable_equal(out.obj, [1, 2])
    out = rep(df2.x, times=df2.each, length=df2.each, each=df2.each)
    assert_iterable_equal(out.obj, [1, 2])
    out = rep(3, each=df2.each)
    assert_iterable_equal(out.obj, [3, 3])

    out = rep(df2.x.obj, 2)
    assert_iterable_equal(out, [1, 2, 1, 2])


def test_rep_df():
    df = tibble(x=c[:3])
    with pytest.raises(ValueError):
        rep(df, each=2)

    out = rep(df, times=2, length=5)
    assert_frame_equal(out, tibble(x=[0, 1, 2, 0, 1]))


def test_rep_grouped_df():
    df = tibble(x=c[:3], g=[1, 1, 2]).group_by("g")
    out = rep(df, 2, length=5)
    assert isinstance(out, TibbleGrouped)
    assert_iterable_equal(out.x.obj, [0, 1, 2, 0, 1])
    assert out._datar["grouped"].grouper.ngroups == 2


def test_rep_error():
    with pytest.raises(ValueError):
        rep(c(1, 2, 3), c(1, 2))
    with pytest.raises(ValueError):
        rep(c(1, 2, 3), c(1, 2, 3), each=2)
