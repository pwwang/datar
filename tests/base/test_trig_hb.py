import pytest

from datar.base import NA
from datar.base.trig_hb import (
    acos,
    acosh,
    asin,
    asinh,
    atan,
    atan2,
    atanh,
    sin,
    sinh,
    sinpi,
    tan,
    tanh,
    tanpi,
    cos,
    cosh,
    cospi,
)
from datar.tibble import tibble
from ..conftest import assert_iterable_equal


@pytest.mark.parametrize('fun,x,exp', [
    (cos, 1, 0.5403023),
    (cos, NA, NA),
    (sin, 1, 0.841471),
    (tan, 1, 1.557408),
    (acos, 1, 0),
    (asin, 1, 1.570796),
    (atan, 1, 0.7853982),
    (cospi, 1, -1),
    (sinpi, 1, 0),
    (tanpi, 1, 0),
    (cosh, 1, 1.543081),
    (sinh, 1, 1.175201),
    (tanh, 1, 0.7615942),
    (acosh, 1, 0),
    (asinh, 1, 0.8813736),
    (atanh, .4, 0.4236489),
])
def test_func_with_1arg(fun, x, exp):
    out = fun(x)
    assert_iterable_equal(out, [exp], approx=True)


def test_atan2():
    assert pytest.approx(atan2(3, 3)) == 0.7853982

    df = tibble(a=[1, 1], b=[3, 4])
    rf = df.rowwise()
    out = atan2(rf.a, rf.b)
    assert_iterable_equal(out.obj, [0.321751, 0.244979], approx=1e-4)
    assert out.is_rowwise

    out = atan2(df.a, df.b)
    assert_iterable_equal(out, [0.321751, 0.244979], approx=1e-4)
    assert_iterable_equal(out.index, [0, 1])
