"""Some constants/alias for R counterparts"""
import math
from string import ascii_letters

import numpy as np


pi = math.pi

letters = np.array(list(ascii_letters[:26]), dtype='<U1')
LETTERS = np.array(list(ascii_letters[26:]), dtype='<U1')


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
    dtype='<U9',
)

month_abb = np.array(month_name, dtype='<U3')
