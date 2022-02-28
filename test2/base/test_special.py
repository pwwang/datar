from numpy import tri
import pytest

from datar2.base.special import *
from datar2.base import Inf, NA
from ..conftest import assert_iterable_equal, is_installed

pytestmark = pytest.mark.skipif(
    not is_installed('scipy'),
    reason="'scipy' is not installed"
)

@pytest.mark.parametrize('a,b,log,exp', [
    (1, 2, False, 0.5),
    ([1,2],2,False,[.5,.1666666666667]),
    (1,[1,2],False,[1.0, 0.5]),
    (1,2,True,-0.6931472),
    ([1,2],2,True,[-0.6931472,-1.7917595]),
    (1,[1,2],True,[0.0, -0.6931472]),
])
def test_beta(a, b, log, exp):
    fun = lbeta if log else beta
    out = fun(a, b)

    if is_scalar(exp):
        out, exp = [out], [exp]
    assert_iterable_equal(out, exp, approx=True)

@pytest.mark.parametrize('x,fun,exp', [
    (-1, gamma, NA),
    (0, gamma, NA),
    ([0,1,2], gamma, [NA, 1, 1]),
    (-1, lgamma, NA),
    (0, lgamma, NA),
    ([0,1,2], lgamma, [NA, 0, 0]),
    (1, digamma, -0.5772157),
    (-1, digamma, NA),
    (0, digamma, NA),
    (1, trigamma, 1.644934),
    (-1, trigamma, Inf),
    (0, trigamma, Inf),
    (0, factorial, 1),
    (-1, factorial, NA),
    ([-1, 1], factorial, [NA, 1]),
    (0, lfactorial, 0),
])
def test_gamma(x, fun, exp):
    out = fun(x)
    if is_scalar(exp):
        out, exp = [out], [exp]
    assert_iterable_equal(exp, out, approx=True)

@pytest.mark.parametrize('n,k,log,exp', [
    (5,2,False,10),
    ([4,5],2,False,[6,10]),
    (5,2,True,2.302585),
    ([4,5],2,True,[1.791759469,2.302585]),
])
def test_choose(n,k,log,exp):
    fun = lchoose if log else choose
    out = fun(n,k)
    if is_scalar(exp):
        out, exp = [out], [exp]
    assert_iterable_equal(exp, out, approx=True)

@pytest.mark.parametrize('x,deriv,exp',[
    (1,0,-0.5772157),
    (2,1,0.6449341),
    (2,-1,NA),
    (-2,1,Inf),
    (1, (0,1), [-0.5772157,1.6449341])
])
def test_psigamma(x, deriv, exp):
    out = psigamma(x, deriv)
    if is_scalar(exp):
        out, exp = [out], [exp]
    assert_iterable_equal(exp, out, approx=True)
