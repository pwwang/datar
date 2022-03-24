from ... import BACKEND

if BACKEND == "pandas":
    from pandas.core.groupby import (
        DataFrameGroupBy,
        GroupBy,
        SeriesGroupBy,
    )

elif BACKEND == "modin":  # pragma: no cover
    from modin.pandas.groupby import (
        DataFrameGroupBy,
        GroupBy,
        SeriesGroupBy,
    )
