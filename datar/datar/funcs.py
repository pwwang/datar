"""Basic functions"""
import operator

from pipda import register_func

# pylint: disable=invalid-name
itemgetter = register_func(None)(operator.itemgetter)
