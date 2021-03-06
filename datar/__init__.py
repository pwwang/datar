"""Load operator, provide f and __version__"""
from pipda import Symbolic

# pylint: disable=unused-import
from .core import operator

f = Symbolic() # pylint: disable=invalid-name

__version__ = '0.0.1'
