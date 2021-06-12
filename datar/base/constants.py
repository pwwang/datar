"""Some constants/alias for R counterparts"""
import math
from string import ascii_letters

import numpy
import pandas

# pylint: disable=invalid-name
NA = pandas.NA
NaN = numpy.nan
TRUE = True
FALSE = False
NULL = None

pi = math.pi
Inf = numpy.inf

letters = pandas.array(list(ascii_letters[:26]))
LETTERS = pandas.array(list(ascii_letters[26:]))

NA_character_ = NA
NA_integer_ = NA
NA_real_ = NaN
NA_compex_ = complex(NA_real_, NA_real_)
