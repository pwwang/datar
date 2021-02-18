import unittest

from numpy.core.numeric import NaN
from plyrda.funcs import *
from plyrda.verbs import *
from plyrda.common import X
from plyrda.data import iris, billboard, starwars, relig_income

class TestFuncs(unittest.TestCase):

    def test_starts_with(self):
        x = iris >> head(10) >> select(starts_with("Sepal"))
        self.assertEqual(x.columns.to_list(), ['Sepal.Length', 'Sepal.Width'])

        x = iris >> head(10) >> select(starts_with(c("Petal", "Sepal")))
        self.assertEqual(x.columns.to_list(),
                         'Petal.Length Petal.Width Sepal.Length Sepal.Width'.split())
        x = iris >> head(10) >> select(starts_with(["Petal", "Sepal"]))
        self.assertEqual(x.columns.to_list(),
                         'Petal.Length Petal.Width Sepal.Length Sepal.Width'.split())

        x = billboard >> head(10) >> select(starts_with("wk"))
        self.assertEqual(x.columns.to_list(),
                         [f'wk{i}' for i in range(1, 77)])


    def test_ends_with(self):
        x = iris >> head(10) >> select(ends_with("Width"))
        self.assertEqual(x.columns.to_list(), ['Sepal.Width', 'Petal.Width'])

        x = iris >> head(10) >> select(ends_with(c("Width", "Length")))
        self.assertEqual(x.columns.to_list(),
                         'Sepal.Width Petal.Width Sepal.Length Petal.Length'.split())
        x = iris >> head(10) >> select(ends_with(["Width", "Length"]))
        self.assertEqual(x.columns.to_list(),
                         'Sepal.Width Petal.Width Sepal.Length Petal.Length'.split())

    def test_contains(self):
        x = iris >> head(10) >> select(contains("al"))
        self.assertEqual(x.columns.to_list(),
                         'Sepal.Length Sepal.Width Petal.Length Petal.Width'.split())

    def test_matches(self):
        x = iris >> head(10) >> select(matches(r"[pt]al"))
        self.assertEqual(x.columns.to_list(),
                         'Sepal.Length Sepal.Width Petal.Length Petal.Width'.split())

    def test_num_range(self):
        x = billboard >> head(10) >> select(num_range("wk", seq(10, 16)))
        self.assertEqual(x.columns.to_list(),
                         [f'wk{i}' for i in range(10, 16)])

    def test_seq(self):
        # see: https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/seq
        self.assertEqual([round(x, 1) for x in seq.pipda(None, 'name', 0, 1.1, length_out=11)],
                         [round(0.0 + n * 0.1, 1) for n in range(11)])

        self.assertEqual(seq.pipda(None, 'name', range(10)),
                         list(range(10)))

        self.assertEqual(seq.pipda(None, 'name', 1, 10, by=2),
                         [1, 3, 5, 7, 9])

        self.assertEqual([round(x, 2) for x in seq.pipda(None, 'name', 1, 10, by=3.14)],
                         [1.00, 4.14, 7.28])

        self.assertEqual(seq.pipda(None, 'name', 1, 7, by=3),
                         [1, 4])

        self.assertEqual([round(x, 3) for x in seq.pipda(None, 'name', 1.575, 5.175, by=0.05)],
                         [round(1.575 + n * 0.05, 3) for n in range(72)])

        self.assertEqual(seq.pipda(None, 'name', 18),
                         list(range(18)))
        self.assertEqual(seq_len.pipda(None, 'name', 18),
                         list(range(18)))

    def test_name_range(self):
        x = starwars >> select(X.height) >> head(2)
        self.assertEqual(x.height.to_list(), [172.0, 167.0])

    def test_ranking(self):
        d = DataFrame({'x': [5, 1, 3, 2, 2, NaN]}).x
        x = row_number.pipda(DataFrame(), None, d)
        self.assertListEqual(x.to_list()[:-1], [5.0, 1.0, 4.0, 2.0, 3.0])
        self.assertTrue(x.isna().to_list()[-1])

        x = min_rank.pipda(DataFrame(), None, d)
        self.assertListEqual(x.to_list()[:-1], [5.0, 1.0, 4.0, 2.0, 2.0])
        self.assertTrue(x.isna().to_list()[-1])

        x = dense_rank.pipda(DataFrame(), None, d)
        self.assertListEqual(x.to_list()[:-1], [4.0, 1.0, 3.0, 2.0, 2.0])
        self.assertTrue(x.isna().to_list()[-1])

        x = percent_rank.pipda(DataFrame(), None, d)
        self.assertListEqual(
            [round(s, 2) for s in x.to_list()[:-1]],
            [1.00, 0.00, 0.75, 0.25, 0.25])
        self.assertTrue(x.isna().to_list()[-1])

        x = cume_dist.pipda(DataFrame(), None, d)
        self.assertListEqual(x.to_list()[:-1], [1.00, 0.20, 0.8, 0.6, 0.6])
        self.assertTrue(x.isna().to_list()[-1])

if __name__ == "__main__":
    unittest.main(verbosity = 2)



