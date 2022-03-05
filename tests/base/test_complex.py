import pytest

from ..conftest import assert_iterable_equal


from datar.base.complex import (
    re,
    im,
    mod,
    arg,
    conj,
    is_complex,
    as_complex,
)


def test_complex():
    x = 1 + 2j
    assert re(x) == 1
    assert im(x) == 2
    assert_iterable_equal(mod(x), [2.236068], approx=1e-5)
    assert_iterable_equal(arg(x), [1.107149], approx=1e-5)
    assert_iterable_equal(conj(x), [1 - 2j])
    assert is_complex(x)
    assert not is_complex(1)
    assert as_complex(1) == 1 + 0j
