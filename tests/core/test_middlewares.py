import pytest  # noqa

from datar.core.middlewares import CurColumn


def test_curcolumn():
    out = CurColumn.replace_args([CurColumn()], 'cur')
    assert out == ('cur', )
