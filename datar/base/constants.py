"""Some constants/alias for R counterparts"""
import math
from string import ascii_letters

import numpy

# pylint: disable=invalid-name

pi = math.pi
Inf = numpy.inf

letters = numpy.array(list(ascii_letters[:26]))
LETTERS = numpy.array(list(ascii_letters[26:]))

month_abb = numpy.array(
    [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
)
month_name = numpy.array(
    [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
)
