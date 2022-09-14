# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-rename.R
import pytest  # noqa
from datar import f
from datar.base import (
    names,
)
from datar.dplyr import (
    rename,
    rename_with,
    group_vars,
    group_by,
)
from datar.tibble import tibble
from datar.datasets import mtcars

from ..conftest import assert_iterable_equal, assert_equal


def test_rename_handles_data_pronoun():
    df = tibble(x=1)
    out = rename(df, y=df.x)
    assert out.equals(tibble(y=1))


def test_rename_with_dict():
    ns = dict(foo="cyl", bar="am")
    out = rename(mtcars, **ns)
    exp = rename(mtcars, foo=f.cyl, bar=f.am)
    assert out.equals(exp)


def test_rename_preserve_grouping():
    gf = group_by(tibble(g=[1, 2, 3], x=[3, 2, 1]), f.g)

    out = rename(gf, h=f.g)
    assert_equal(group_vars(out), ["h"])


def test_can_rename_with_duplicate_columns():
    df = tibble(tibble(x=1), x=2, y=1, _name_repair="minimal")
    out = df >> rename(x2=1) >> names()
    assert_iterable_equal(out, ["x", "x2", "y"])


# # rename_with -------------------------------------------------------------


def test_can_select_columns():
    df = tibble(x=1, y=2)
    out = df >> rename_with(str.upper) >> names()
    assert_iterable_equal(out, ["X", "Y"])
    out = df >> rename_with(str.upper, 0) >> names()
    assert_iterable_equal(out, ["X", "y"])
    out = df >> rename_with(str.upper, f.x) >> names()
    assert_iterable_equal(out, ["X", "y"])


def test_passes_args_kwargs_along():
    df = tibble(x=1, y=2)
    out = df >> rename_with(str.replace, f.x, "x", "X") >> names()
    assert_iterable_equal(out, ["X", "y"])


def test_cannot_create_duplicate_names():
    df = tibble(x=1, y=2)
    df >> rename_with(lambda n: "X", [f.x, f.y])
