from .core.backends.pandas import DataFrame
from .core.backends.pandas.testing import assert_frame_equal, assert_index_equal

from .core.tibble import TibbleGrouped


def assert_tibble_equal(df1, df2):
    assert isinstance(df1, DataFrame), "First value is not a data frame."
    assert isinstance(df2, DataFrame), "Second value is not a data frame."

    assert type(df1) is type(df2), (
        f"Not the same class: {type(df1).__name__}, "
        f"{type(df2).__name__}"
    )
    assert_frame_equal(df1, df2)
    if isinstance(df1, TibbleGrouped):
        assert_index_equal(
            df1._datar["grouped"].grouper.result_index,
            df2._datar["grouped"].grouper.result_index
        )
