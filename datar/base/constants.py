"""Some constants/alias for R counterparts"""
import math
from string import ascii_letters

import numpy

NA = numpy.nan
TRUE = True
FALSE = False
NULL = None

pi = math.pi # pylint: disable=invalid-name
Inf = numpy.inf # pylint: disable=invalid-name

letters = list(ascii_letters[:26]) # pylint: disable=invalid-name
LETTERS = list(ascii_letters[26:])
