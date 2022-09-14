from datar.core.backends.pandas import Categorical
from datar import f
from datar.base import c, as_date, rep
from datar.datar import (
    get,
    flatten,
    itemgetter,
    attrgetter,
    pd_str,
    pd_cat,
    pd_dt,
)
from datar.dplyr import group_by, mutate, summarise
from datar.tibble import tibble
from datar.core.backends.pandas.testing import assert_frame_equal

from .conftest import assert_iterable_equal


def test_itemgetter():
    arr = [1, 2, 3]
    df = tibble(x=2)
    out = df >> mutate(y=itemgetter(arr, f.x))
    assert_frame_equal(out, tibble(x=2, y=3))

    df = tibble(x=c[1:9], y=rep(c[1:3], each=4)) >> group_by(f.y)
    out = itemgetter(df.x, c[:2])
    assert_iterable_equal(out.obj, [1, 2, 5, 6])

def test_get():
    df = tibble(x=2)
    df.index = ["a"]

    out = df >> get()
    assert_frame_equal(out, df)

    out = df >> get(0, 0)
    assert out == 2

    out = df >> get("a", "x")
    assert out == 2

    out = df >> get(["a"], ["x"])
    assert out.equals(df)

    out = df >> get("a")
    assert out.equals(df)

    out = df >> get(cols="x")
    assert out.equals(df)


def test_flatten():
    df = tibble(x=[1, 2], y=[3, 4])
    out = df >> flatten(True)
    assert out == [1, 2, 3, 4]
    out = df >> flatten()
    assert out == [1, 3, 2, 4]


def test_attrgetter():
    df = tibble(x=list("abc"))

    out = df >> mutate(y=attrgetter(f.x, "str").upper())
    assert_iterable_equal(out.y, ["A", "B", "C"])

    out = df >> mutate(y=pd_str(f.x).upper())
    assert_iterable_equal(out.y, ["A", "B", "C"])

    gf = df >> group_by(g=1)
    out = gf >> mutate(y=attrgetter(f.x, "str").upper())
    assert_iterable_equal(out.y.obj, ["A", "B", "C"])

    out = gf >> mutate(y=pd_str(f.x).upper())
    assert_iterable_equal(out.y.obj, ["A", "B", "C"])


def test_pd_str():
    df = tibble(x=["ab", "bc"]) >> group_by(g=[1, 2])
    out = pd_str(df.x)[:1]

    assert_iterable_equal(out.obj, ["a", "b"])


def test_pd_cat():
    df = tibble(
        x=Categorical(["a", "b"], categories=["a", "b", "c"])
    ) >> group_by(g=[1, 2])
    out = df >> summarise(lvls=pd_cat(f.x).categories)
    print(out)
    assert_iterable_equal(out.lvls[0], ["a", "b", "c"])
    assert_iterable_equal(out.lvls[1], ["a", "b", "c"])


def test_pd_dt():
    df = (
        tibble(x=["2022-01-01", "2022-12-12"])
        >> mutate(x=as_date(f.x, format="%Y-%m-%d"))
        >> group_by(g=[1, 2])
    )
    out = pd_dt(df.x).month

    assert_iterable_equal(out.obj, [1, 12])
