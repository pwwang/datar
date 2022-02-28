import pytest

from datar.base.complex import *
from ..conftest import assert_iterable_equal


def test_complex():
    x = 1 + 2j
    assert re(x) == 1
    assert im(x) == 2
    assert mod(x) == pytest.approx(2.236068)
    assert arg(x) == pytest.approx(1.107149)
    assert conj(x) == 1 - 2j
    assert is_complex(x)
    assert not is_complex(1)
    assert as_complex(1) == 1 + 0j
