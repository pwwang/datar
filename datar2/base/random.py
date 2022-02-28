"""Functions related to randomization"""
import random as std_random

import numpy


def set_seed(seed: int) -> None:
    """Set the seed for randomization"""
    std_random.seed(seed)
    numpy.random.seed(seed)
