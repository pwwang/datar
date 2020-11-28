import unittest

from pandas.api.types import is_string_dtype, is_numeric_dtype

from plyrda.funcs import *
from plyrda.verbs import *
from plyrda.common import *
from plyrda.data import diamonds, relig_income, billboard, who, anscombe

class TestVerbs(unittest.TestCase):

    def test_head(self):
        x = diamonds >> head(3)
        self.assertEqual(x.shape[0], 3)
        self.assertEqual(x.cut.to_list(), ['Ideal', 'Premium', 'Good'])

    def test_tail(self):
        x = diamonds >> head(10) >> tail(3)
        self.assertEqual(x.shape[0], 3)
        self.assertEqual(x.color.to_list(), ['H', 'E', 'H'])

    def test_select(self):
        x = diamonds >> select(1, X.price, ['x', 'y']) >> head(2)
        self.assertEqual(x.columns.to_list(), ['cut', 'price', 'x', 'y'])
        self.assertEqual(x.shape, (2, 4))

        x = diamonds >> select(c(X.cut, X.price)) >> head(2)
        self.assertEqual(x.columns.to_list(), ['cut', 'price'])

        x = diamonds >> select(-c(X.cut, X.price)) >> head(2)
        self.assertNotIn('cut', x.columns.to_list())
        self.assertNotIn('price', x.columns.to_list())

        x = diamonds >> select(-X.price) >> head(2)
        self.assertNotIn('price', x.columns.to_list())

        x = diamonds >> select(starts_with('c')) >> head(2)
        self.assertEqual(x.columns.to_list(), ['carat','cut', 'color', 'clarity'])

        x = diamonds >> select(-starts_with('c')) >> head(2)
        self.assertEqual(x.columns.to_list(), ['depth', 'table', 'price', 'x', 'y', 'z'])

        x = diamonds >> select(columns_to(1, inclusive=True), 'depth', columns_from(-2)) >> head(2)
        self.assertEqual(x.columns.to_list(), ['carat', 'cut', 'depth', 'y', 'z'])

        with self.assertRaises(PlyrdaColumnNameInvalid):
            diamonds >> select(X.cut, -X.price)

    def test_drop(self):
        x = diamonds >> drop(columns_from(X.price)) >> head(2)
        self.assertEqual(x.columns.to_list(), ['carat', 'cut', 'color', 'clarity', 'depth', 'table'])

    def test_relocate(self):
        # https://dplyr.tidyverse.org/reference/relocate.html
        df = DataFrame(dict(a=1, b=1, c=1, d="a", e="a", f="a"), index=[0])

        x = df >> relocate(X.f)
        self.assertEqual(x.columns.to_list(), ['f', 'a', 'b', 'c', 'd', 'e'])

        x = df >> relocate(X.a, _after=X.c)
        self.assertEqual(x.columns.to_list(), ['b', 'c', 'a', 'd', 'e', 'f'])

        x = df >> relocate(X.f, _before=X.b)
        self.assertEqual(x.columns.to_list(), ['a', 'f', 'b', 'c', 'd', 'e'])

        x = df >> relocate(X.a, _after=last_col())
        self.assertEqual(x.columns.to_list(), ['b', 'c', 'd', 'e', 'f', 'a'])

        x = df >> relocate(where(is_string_dtype))
        self.assertEqual(x.columns.to_list(), ['d', 'e', 'f', 'a', 'b', 'c'])

        x = df >> relocate(where(is_numeric_dtype), _after=last_col())
        self.assertEqual(x.columns.to_list(), ['d', 'e', 'f', 'a', 'b', 'c'])


    def test_pivot_longer(self):
        x = relig_income >> pivot_longer(-X.religion, names_to="income", values_to="count") >> head(4)
        self.assertEqual(x.religion.to_list(), ['Agnostic', 'Atheist', 'Buddhist', 'Catholic'])
        self.assertEqual(x.income.to_list(), ['<$10k'] * 4)
        self.assertEqual(x['count'].to_list(), [27, 12, 27, 418])

        x = billboard >> pivot_longer(
            cols=starts_with("wk"),
            names_to="week",
            names_prefix="wk",
            values_to="rank",
            values_drop_na = True
        ) >> head(4)
        self.assertEqual(x['artist'].to_list(), ['2 Pac', '2Ge+her', '3 Doors Down', '3 Doors Down'])
        self.assertEqual(x['track'].to_list(), ["Baby Don't Cry (Keep...", "The Hardest Part Of ...", "Kryptonite", "Loser"])
        self.assertEqual(x['date.entered'].to_list(), ["2000-02-26", "2000-09-02", "2000-04-08", "2000-10-21"])
        self.assertEqual(x['week'].to_list(), ["1"] * 4)
        self.assertEqual(x['rank'].to_list(), [87.0, 91.0, 81.0, 76.0])

        x = who >> pivot_longer(
            cols=columns_between(X.new_sp_m014, X.newrel_f65),
            names_to=c("diagnosis", "gender", "age"),
            names_pattern = r"new_?(.*)_(.)(.*)",
            values_to = "count"
        ) >> head()
        self.assertEqual(x['country'].to_list(), ['Afghanistan'] * 5)
        self.assertEqual(x['age'].to_list(), ['014'] * 5)

        x = anscombe >> pivot_longer(everything(),
            names_to = c(".value", "set"),
            names_pattern = r"(.)(.)",
            values_ptypes = {'set': int}
        ) >> head()
        self.assertEqual(x.x.to_list(), [10.0, 8.0, 13.0, 9.0, 11.0])
        self.assertEqual(x.y.to_list(), [8.04, 6.95, 7.58, 8.81, 8.33])
        self.assertEqual(x['set'].to_list(), [1] * 5)

if __name__ == "__main__":
    unittest.main()