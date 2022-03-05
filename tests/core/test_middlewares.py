import pytest  # noqa

from pipda.utils import NULL
from datar.core.middlewares import CurColumn, WithDataEnv


def test_curcolumn():
    out = CurColumn.replace_args([CurColumn()], 'cur')
    assert out == ('cur', )
    out = CurColumn.replace_kwargs({"kw": CurColumn()}, 'cur')
    assert out == {"kw": "cur"}


def test_withdataenv():

    de = WithDataEnv(1)
    with de:
        assert de.data.data == 1

    assert de.data.data is NULL
