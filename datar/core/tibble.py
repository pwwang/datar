"""Provide Tibble class, which is actually a DataFrame with enriched functions
"""
from pandas import DataFrame

class Tibble(DataFrame):
    """DataFrame with enriched functions

    Including:
    1. dtypes printed when stringified
    2. copy() keeps Tibble class
    3. nested data frame not expanded when printing
    """