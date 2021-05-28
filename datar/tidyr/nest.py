# """Nest and unnest

# https://github.com/tidyverse/tidyr/blob/master/R/nest.R
# """
# from typing import Optional, Union

# from pandas import DataFrame
# from pipda import register_verb

# from ..core.utils import logger


# @register_verb(DataFrame)
# def nest(
#         _data: DataFrame,
#         _names_sep: Optional[str] = None,
#         **cols: Union[str, int]
# ) -> DataFrame:
#     """Nesting creates a list-column of data frames

#     Args:
#         _data: A data frame
#         *cols: Columns to nest
#         _names_sep: If `None`, the default, the names will be left as is.
#             Inner names will come from the former outer names
#             If a string, the inner and outer names will be used together.
#             The names of the new outer columns will be formed by pasting
#             together the outer and the inner column names, separated by
#             `_names_sep`.

#     Returns:
#         Nested data frame.
#     """
