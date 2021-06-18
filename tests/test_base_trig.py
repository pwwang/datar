import pytest

from datar.base import NA
from datar.base.trig import *
from datar.core.types import is_scalar
from .conftest import assert_iterable_equal

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
])
def test_func_with_1arg(fun, x, exp):
    out = fun(x)
    if is_scalar(exp):
        out, exp = [out], [exp]
    assert_iterable_equal(out, exp, approx=True)

def test_atan2():
    assert pytest.approx(atan2(3,3)) == 0.7853982
