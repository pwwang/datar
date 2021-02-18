import warnings
import pytest
from pandas import DataFrame
from plyrda.commons import *
from plyrda.data import iris

@pytest.fixture(autouse=True)
def no_warnings():
    warnings.simplefilter('ignore')

def test_is_numeric():
    df = DataFrame({'a': [1], 'b': ['c']})
    assert is_numeric(df.a).all()
    assert not is_numeric(df.b).all()
    assert is_numeric(1).all()
    assert not is_numeric('a').all()

    assert is_numeric(iris['Sepal.Length']).all()
    assert not is_numeric(iris['Species']).all()

@pytest.mark.parametrize("indate, format, optional, tz, origin, outdate", [
    (["1jan1960", "2jan1960", "31mar1960", "30jul1960"],
     "%d%b%Y", None, 0, None,
     ["1960-01-01", '1960-01-02', '1960-03-31', '1960-07-30']),
    (["02/27/92", "02/27/92", "01/14/92", "02/28/92", "02/01/92"],
     "%m/%d/%y", None, 0, None,
     ['1992-02-27', '1992-02-27', '1992-01-14', '1992-02-28', '1992-02-01']),
    (["04/13/2010 00:00:00", "04/13/2010 12:00:00"],
     "%m/%d/%Y %H:%M:%S", None, 0, None,
     ['2010-04-13', '2010-04-13']),
    (["04/13/2010 00:00:00", "04/13/2010 12:00:00"],
     "%m/%d/%Y %H:%M:%S", None, 13, None,
     ['2010-04-13', '2010-04-14']),
    (["04/13/2010 00:00:00", "04/13/2010 12:00:00"],
     "%m/%d/%Y %H:%M:%S", None, -1, None,
     ['2010-04-12', '2010-04-13']),
    (32768,
     None, None, 0, "1900-01-01",
     ['1989-09-19']),
    (35981,
     None, None, 0, "1899-12-30",
     ['1998-07-05']),
    (34519,
     None, None, 0, "1904-01-01",
     ['1998-07-05']),
    (734373,
     None, None, 0, "1970-01-01",
     ['3980-08-24']),
])
def test_as_date(indate, format, optional, tz, origin, outdate):
    dateobj = as_date(indate,
                      format=format,
                      optional=optional,
                      tz=tz,
                      origin=origin)
    if not dateobj.shape:
        dateobj = [dateobj]
    assert [str(dt) for dt in dateobj] == outdate
