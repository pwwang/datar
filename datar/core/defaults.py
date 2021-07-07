"""Some default values used in datar"""
from pipda import Symbolic

# When unable to retrieve the expression for column name
# For example:
# >>> df >> mutate(f.x+1)
# In such a case, `f.x+1` cannot be fetched, we use `Var0` instead.
DEFAULT_COLUMN_PREFIX = "_Var"
NA_REPR = "<NA>"

f = Symbolic()  # pylint: disable=invalid-name
