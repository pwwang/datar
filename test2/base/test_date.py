import pytest

import pandas
from datar2.base.date import *
from ..conftest import assert_iterable_equal

@pytest.mark.parametrize('x,format,try_formats,optional,tz,origin,expected', [
    (["1jan1960", "2jan1960", "31mar1960", "30jul1960"],
     "%d%b%Y", None, False, 0, None,
     [datetime.date(1960, 1, 1),
      datetime.date(1960, 1, 2),
      datetime.date(1960, 3, 31),
      datetime.date(1960, 7, 30)]),
    (["02/27/92", "02/27/92", "01/14/92", "02/28/92", "02/01/92"],
     "%m/%d/%y", None, False, 0, None,
     [datetime.date(1992, 2, 27),
      datetime.date(1992, 2, 27),
      datetime.date(1992, 1, 14),
      datetime.date(1992, 2, 28),
      datetime.date(1992, 2, 1)]),
    (32768,
     None, None, False, 0, "1900-01-01",
     [datetime.date(1989, 9, 19)]),
    (35981,
     None, None, False, 0, "1899-12-30",
     [datetime.date(1998, 7, 5)]),
    (34519,
     None, None, False, 0, "1904-01-01",
     [datetime.date(1998, 7, 5)]),
    (734373 - 719529,
     None, None, False, 0, datetime.datetime(1970, 1, 1),
     [datetime.date(2010, 8, 23)]),
    ([datetime.date(2010, 4, 13)],
     None, None, False, 12, None,
     [datetime.date(2010, 4, 13)]),
    ([datetime.date(2010, 4, 13)],
     None, None, False, 12 + 13, None,
     [datetime.date(2010, 4, 14)]),
    ([datetime.datetime(2010, 4, 13)],
     None, None, False, 0 - 10, None,
     [datetime.date(2010, 4, 12)]),
])
def test_as_date(x, format, try_formats, optional, tz, origin, expected):
    out = as_date(
        x,
        format,
        try_formats,
        optional,
        tz,
        origin
    )
    if out.size == 1:
        out = out.ravel()
    assert_iterable_equal(as_date(
        x,
        format,
        try_formats,
        optional,
        tz,
        origin
    ).ravel(), pandas.to_datetime(expected))

def test_as_date_error():
    with pytest.raises(ValueError):
        as_date(1.1)

    with pytest.raises(ValueError):
        as_date("1990-1-1", "%Y")

    with pytest.raises(RuntimeWarning):
        as_date("1990-1-1", "Y", optional=True)

def test_as_pd_date():

    assert as_pd_date("Sep 16, 2021") == pandas.Timestamp('2021-09-16 00:00:00')
