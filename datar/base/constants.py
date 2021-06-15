"""Some constants/alias for R counterparts"""
import math
from string import ascii_letters

import numpy
import pandas

from ..core.defaults import NA_REPR

# pylint: disable=invalid-name
NA = numpy.nan
NaN = pandas.NA
TRUE = True
FALSE = False
NULL = None

pi = math.pi
Inf = numpy.inf

letters = numpy.array(list(ascii_letters[:26]))
LETTERS = numpy.array(list(ascii_letters[26:]))

NA_character_ = NA_REPR
NA_integer_ = numpy.random.randint(numpy.iinfo(numpy.int32).max)
NA_real_ = NA
NA_compex_ = complex(NA_real_, NA_real_)
