# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-n_distinct.R
import numpy
from datar.all import *
from datar2.datasets import iris

df_var = tibble(
    l = c(True, False, False),
    i = c(1, 1, 2),
    # d = Sys.Date() + c(1, 1, 2),
    f = factor(letters[c(1, 1, 2)]),
    n = numpy.array(c(1, 1, 2)) + 0.5,
    # t = Sys.time() + c(1, 1, 2),
    c = letters[c(1, 1, 2)],
)

def test_n_disinct_gives_the_correct_results_on_iris():
    out = iris.apply(n_distinct)
    exp = iris.apply(lambda col: len(col.unique()))
    assert out.tolist() == exp.tolist()

def test_n_distinct_treats_na_correctly():
    # test_that("n_distinct treats NA correctly in the REALSXP case (#384)", {
    assert n_distinct(c(1.0, NA, NA)) == 2

def test_n_distinct_recyles_len1_vec():
    assert n_distinct(1, [1,2,3,4]) == 4
    assert n_distinct([1,2,3,4], 1) == 4

    d = tibble(x=[1,2,3,4])
    res = d >> summarise(
        y=sum(f.x),
        # summrise fail to mix input and summarised data in one expression
        # n1=n_distinct(f.y, f.x),
        # n2=n_distinct(f.x, f.y),
        n3=n_distinct(f.y),
        n4=n_distinct(identity(f.y)),
        n5=n_distinct(f.x)
    )
    # assert res.n1.tolist() == [4]
    # assert res.n2.tolist() == [4]
    assert res.n3.tolist() == [1]
    assert res.n4.tolist() == [1]
    assert res.n5.tolist() == [4]

    res = tibble(g=c(1,1,1,1,2,2), x=c(1,2,3,1,1,2)) >> group_by(
        f.g) >> summarise(
            y=sum(f.x),
            # n1=n_distinct(f.y, f.x),
            # n2=n_distinct(f.x, f.y),
            n3=n_distinct(f.y),
            n4=n_distinct(identity(f.y)),
            n5=n_distinct(f.x)
        )
    # assert res.n1.tolist() == [3,2]
    # assert res.n2.tolist() == [3,2]
    assert res.n3.tolist() == [1,1]
    assert res.n4.tolist() == [1,1]
    assert res.n5.tolist() == [3,2]

def test_n_distinct_handles_unnamed():
    x = iris.Sepal_Length
    y = iris.Sepal_Width

    out = n_distinct(iris.Sepal_Length, iris.Sepal_Width)
    exp = n_distinct(x, y)
    assert out == exp

def test_n_distinct_handles_in_na_rm():
    d = tibble(x=c([1,2,3,4], NA))
    yes = True
    no = False

    out = d >> summarise(n=n_distinct(f.x, na_rm=True)) >> pull(to='list')
    assert out == [4]
    out = d >> summarise(n=n_distinct(f.x, na_rm=False)) >> pull(to='list')
    assert out == [5]
    out = d >> summarise(n=n_distinct(f.x, na_rm=yes)) >> pull(to='list')
    assert out == [4]
    out = d >> summarise(n=n_distinct(f.x, na_rm=no)) >> pull(to='list')
    assert out == [5]

    out = d >> summarise(n=n_distinct(f.x, na_rm=True or True)) >> pull(to='list')
    assert out == [4]

def test_n_distinct_respects_data():
    df = tibble(x=42)
    out = df >> summarise(n=n_distinct(df.x))
    exp = tibble(n=1)
    assert out.equals(exp)

def test_n_distinct_works_with_str_col():
    wrapper = lambda data, col: summarise(
        data,
        result=n_distinct(f[col], na_rm=True)
    )

    df = tibble(x=[1,1,3,NA])
    out = wrapper(df, 'x')
    exp = tibble(result=2)
    assert out.equals(exp)
