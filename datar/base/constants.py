"""Some constants/alias for R counterparts"""
import math
import uuid
from string import ascii_letters

import numpy

NA = numpy.nan
TRUE = True
FALSE = False
NULL = None

# pylint: disable=invalid-name
pi = math.pi
Inf = numpy.inf

letters = numpy.array(list(ascii_letters[:26])) # pylint: disable=invalid-name
LETTERS = numpy.array(list(ascii_letters[26:]))

NA_character_ = f"<NA_{uuid.uuid4()}_>"
NA_integer_ = numpy.random.randint(numpy.iinfo(numpy.int64).max)
NA_real_ = NA
NA_compex_ = complex(NA, NA)
