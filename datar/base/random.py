"""Functions related to randomization"""
import random as std_random

import numpy as np


def set_seed(seed):
    """Set the seed for randomization"""
    std_random.seed(seed)
    np.random.seed(seed)
