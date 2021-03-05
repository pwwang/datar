from plyrda.exceptions import ColumnNotExistingError
import pytest

from plyrda.operator import *
from plyrda.datasets import mtcars
from plyrda.funcs import contains
from plyrda.utils import f

def test_range():
    # mpg  cyl   disp   hp  drat     wt   qsec  vs  am  gear  carb
    r = contains('m', vars=f.cyl // f.am, _calling_type='piping').evaluate(mtcars)
    assert r == ['am']

    r = contains('m', vars=None // f.am, _calling_type='piping').evaluate(mtcars)
    assert r == ['mpg', 'am']

    r = contains('m', vars=None // f.hp, _calling_type='piping').evaluate(mtcars)
    assert r == ['mpg']

    r = contains('m', vars=f.mpg // None, _calling_type='piping').evaluate(mtcars)
    assert r == ['mpg', 'am']

    with pytest.raises(ColumnNotExistingError):
        contains('m', vars=f.a // None, _calling_type='piping').evaluate(mtcars)

    with pytest.raises(ColumnNotExistingError):
        contains('m', vars=None // f.a, _calling_type='piping').evaluate(mtcars)
