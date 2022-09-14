import pytest  # noqa

from datar.core.backends.pandas.testing import assert_frame_equal
from datar.all import (
    f,
    c,
    tibble,
    mutate,
    select,
    summarise,
)
from tests.conftest import assert_iterable_equal


def test_neg():
    df = tibble(x=[1, 2])
    out = df >> mutate(y=-f.x)
    assert_frame_equal(out, tibble(x=[1, 2], y=[-1, -2]))


def test_or_():
    df = tibble(x=1, y=2, z=3)
    out = df >> select(c(f.x, f.y) | [f.y, f.z])
    assert_frame_equal(out, tibble(x=1, y=2, z=3))


def test_ne():
    df = tibble(x=[1, 2], y=[1, 3])
    out = mutate(df, z=f.x != f.y)
    assert_frame_equal(out, tibble(x=[1, 2], y=[1, 3], z=[False, True]))
    out = mutate(df, z=f.x.size != f.y.size)
    assert_frame_equal(out, tibble(x=[1, 2], y=[1, 3], z=[False, False]))


def test_undefined_op():
    df = tibble(x=[1, 2], y=[1, 3])
    out = mutate(df, z=f.x * f.y)
    assert_frame_equal(out, tibble(x=[1, 2], y=[1, 3], z=[1, 6]))


def test_op_getattr():
    df = tibble(x=[1, 2], y=[1, -3])
    out = mutate(df, z=(f.x * f.y).abs())
    assert_frame_equal(out, tibble(x=[1, 2], y=[1, -3], z=[1, 6]))


def test_right_recycle_to_left():
    df = tibble(x=[True, False])
    out = mutate(df, y=f.x | True)
    assert_frame_equal(out, tibble(x=[True, False], y=[True, True]))


def test_rowwise_gets_rowwise():
    df = tibble(x=[1, 2, 3], y=[4, 5, 6]).rowwise()
    out = mutate(df, z=1 + f.y, w=-f.x, t=+f.y)
    assert out.z.is_rowwise
    assert out.w.is_rowwise


def test_inv():
    df = tibble(x=1, y=2)
    out = df >> select(~f.x)
    assert out.columns.tolist() == ["y"]

    df = tibble(x=True)
    out = df >> mutate(y=~f.x)
    assert out.y.tolist() == [False]


def test_neg():
    df = tibble(x=1, y=2)
    out = df >> select(-c[:1])
    assert out.columns.tolist() == ["y"]

    out = df >> summarise(z=-c(f.x, f.y))
    assert_iterable_equal(out.z, [-1, -2])


def test_and_or():
    df = tibble(x=1, y=2, z=3, w=4)
    out = df >> select(c(f.x, f.y) & c(f.y, f.z))
    assert out.columns.tolist() == ["y"]

    out = df >> mutate(a=f.x & f.y)
    assert out.a.tolist() == [True]

    out = df >> mutate(a=True & f.y)
    assert out.a.tolist() == [True]

    out = df >> mutate(a=True | f.y)
    assert out.a.tolist() == [True]

    out = df >> select(c(f.x, f.y) | c(f.y, f.z))
    assert out.columns.tolist() == ["x", "y", "z"]
