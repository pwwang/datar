"""Some constants/alias for R counterparts"""
import math
from string import ascii_letters

import numpy as np


pi = math.pi

letters = np.array(list(ascii_letters[:26]))
LETTERS = np.array(list(ascii_letters[26:]))

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
    ]
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
    ]
)
