import pytest

from pipda import register_verb
from datar.all import *
from datar.core.contexts import Context
from datar.core.exceptions import ColumnNotExistingError

def test_context_refer_nonexisting_col():
    @register_verb(context=Context.EVAL)
    def verb(data, col):
        return col

    df = {'x': 1}
    with pytest.raises(ColumnNotExistingError):
        verb(df, f.y)
