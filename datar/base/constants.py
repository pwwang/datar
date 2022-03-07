"""Some constants/alias for R counterparts"""
import math
from string import ascii_letters

import numpy as np


pi = math.pi

letters = np.array(list(ascii_letters[:26]), dtype=object)
LETTERS = np.array(list(ascii_letters[26:]), dtype=object)

month_abb = np.array(
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
    ],
    dtype=object,
)
month_name = np.array(
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
    ],
    dtype=object,
)
