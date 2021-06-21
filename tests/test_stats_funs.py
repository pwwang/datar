import pytest

from datar.stats.funs import *
from datar.base import NA

from .conftest import assert_iterable_equal

def test_weighted_mean():
    assert pytest.approx(weighted_mean([1,2,3], [1,2,3])) == 2.3333333
    assert weighted_mean(1, 1) == 1
    assert weighted_mean([1, NA], [1, NA], na_rm=True) == 1
    assert_iterable_equal([weighted_mean([1,2], [-1, 1])], [NA])
    with pytest.raises(ValueError):
        weighted_mean([1,2], [1,2,3])
