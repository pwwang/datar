import pytest

from plyrda.operator import *
from plyrda.data import mtcars
from plyrda.funcs import contains
from plyrda.utils import f

def test_range():
    # mpg  cyl   disp   hp  drat     wt   qsec  vs  am  gear  carb
    r = contains('m', vars=f.cyl // f.am, _force_piping=True).evaluate(mtcars)
    assert r == ['am']

    r = contains('m', vars=None // f.am, _force_piping=True).evaluate(mtcars)
    assert r == ['mpg', 'am']

    r = contains('m', vars=None // f.hp, _force_piping=True).evaluate(mtcars)
    assert r == ['mpg']

    r = contains('m', vars=f.mpg // None, _force_piping=True).evaluate(mtcars)
    assert r == ['mpg', 'am']

    with pytest.raises(ColumnNotExistingError):
        contains('m', vars=f.a // None, _force_piping=True).evaluate(mtcars)

    with pytest.raises(ColumnNotExistingError):
        contains('m', vars=None // f.a, _force_piping=True).evaluate(mtcars)
