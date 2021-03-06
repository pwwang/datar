# ranking
Windowed rank functions.

`row_number`, `ntile`, `min_rank`, `dense_rank`, `percent_rank`, `cume_dist`.

See dplyr reference: [https://dplyr.tidyverse.org/reference/ranking.html](https://dplyr.tidyverse.org/reference/ranking.html)

## Side-by-side comparison

<table width=100%>
<tr>
<th> datar </th>
<th> dplyr </th>
</tr>
<tr valign="top">
<td>
<tr valign="top">
<td>

```python
d = DataFrame({'x': [5, 1, 3, 2, 2, NaN]})

# You only need to call it: row_number() in piping
row_number.pipda(d, None, d.x)
# 0    5.0
# 1    1.0
# 2    4.0
# 3    2.0
# 4    3.0
# 5    NaN
# Name: x, dtype: float64

min_rank.pipda(d, None, d.x)
# 0    5.0
# 1    1.0
# 2    4.0
# 3    2.0
# 4    2.0
# 5    NaN
# Name: x, dtype: float64

dense_rank.pipda(d, None, d.x)
# 0    4.0
# 1    1.0
# 2    3.0
# 3    2.0
# 4    2.0
# 5    NaN
# Name: x, dtype: float64

percent_rank.pipda(d, None, d.x)
# 0    1.00
# 1    0.00
# 2    0.75
# 3    0.25
# 4    0.25
# 5     NaN
# Name: x, dtype: float64

cume_dist.pipda(d, None, d.x)
# 0    1.0
# 1    0.2
# 2    0.8
# 3    0.6
# 4    0.6
# 5    NaN
# Name: x, dtype: float64

```

</td>
<td>

```R
x <- c(5, 1, 3, 2, 2, NA)
row_number(x)
#> [1]  5  1  4  2  3 NA
min_rank(x)
#> [1]  5  1  4  2  2 NA
dense_rank(x)
#> [1]  4  1  3  2  2 NA
percent_rank(x)
#> [1] 1.00 0.00 0.75 0.25 0.25   NA
cume_dist(x)
#> [1] 1.0 0.2 0.8 0.6 0.6  NA
```

</td>
</tr>
<tr valign="top">
<td>

```python
ntile.pipda(None, None, d.x, 2)
# 0      1
# 1      0
# 2      1
# 3      0
# 4      0
# 5    NaN
# Name: x, dtype: category
# Categories (2, int64): [0 < 1]

# slightly differently using pandas.cut
ntile.pipda(None, None, [1,2,3,4,5,6,7,8], 3)
# [0, 0, 0, 1, 1, 2, 2, 2]
# Categories (3, int64): [0 < 1 < 2]
```

</td>
<td>

```R
ntile(x, 2)
#> [1]  2  1  2  1  1 NA
ntile(1:8, 3)
#> [1] 1 1 1 2 2 2 3 3
```

</td>
</tr>
<tr valign="top">
<td>

```python
# keyword is required for mutate
mtcars >> mutate(row_num=row_number() == 0)
#                       mpg  cyl   disp   hp  drat     wt   qsec  vs  am  gear  \
# Mazda RX4            21.0    6  160.0  110  3.90  2.620  16.46   0   1     4
# Mazda RX4 Wag        21.0    6  160.0  110  3.90  2.875  17.02   0   1     4
# Datsun 710           22.8    4  108.0   93  3.85  2.320  18.61   1   1     4
# Hornet 4 Drive       21.4    6  258.0  110  3.08  3.215  19.44   1   0     3
# Hornet Sportabout    18.7    8  360.0  175  3.15  3.440  17.02   0   0     3
# Valiant              18.1    6  225.0  105  2.76  3.460  20.22   1   0     3
# Duster 360           14.3    8  360.0  245  3.21  3.570  15.84   0   0     3
# Merc 240D            24.4    4  146.7   62  3.69  3.190  20.00   1   0     4
# Merc 230             22.8    4  140.8   95  3.92  3.150  22.90   1   0     4
# Merc 280             19.2    6  167.6  123  3.92  3.440  18.30   1   0     4
# Merc 280C            17.8    6  167.6  123  3.92  3.440  18.90   1   0     4
# Merc 450SE           16.4    8  275.8  180  3.07  4.070  17.40   0   0     3
# Merc 450SL           17.3    8  275.8  180  3.07  3.730  17.60   0   0     3
# Merc 450SLC          15.2    8  275.8  180  3.07  3.780  18.00   0   0     3
# Cadillac Fleetwood   10.4    8  472.0  205  2.93  5.250  17.98   0   0     3
# Lincoln Continental  10.4    8  460.0  215  3.00  5.424  17.82   0   0     3
# Chrysler Imperial    14.7    8  440.0  230  3.23  5.345  17.42   0   0     3
# Fiat 128             32.4    4   78.7   66  4.08  2.200  19.47   1   1     4
# Honda Civic          30.4    4   75.7   52  4.93  1.615  18.52   1   1     4
# Toyota Corolla       33.9    4   71.1   65  4.22  1.835  19.90   1   1     4
# Toyota Corona        21.5    4  120.1   97  3.70  2.465  20.01   1   0     3
# Dodge Challenger     15.5    8  318.0  150  2.76  3.520  16.87   0   0     3
# AMC Javelin          15.2    8  304.0  150  3.15  3.435  17.30   0   0     3
# Camaro Z28           13.3    8  350.0  245  3.73  3.840  15.41   0   0     3
# Pontiac Firebird     19.2    8  400.0  175  3.08  3.845  17.05   0   0     3
# Fiat X1-9            27.3    4   79.0   66  4.08  1.935  18.90   1   1     4
# Porsche 914-2        26.0    4  120.3   91  4.43  2.140  16.70   0   1     5
# Lotus Europa         30.4    4   95.1  113  3.77  1.513  16.90   1   1     5
# Ford Pantera L       15.8    8  351.0  264  4.22  3.170  14.50   0   1     5
# Ferrari Dino         19.7    6  145.0  175  3.62  2.770  15.50   0   1     5
# Maserati Bora        15.0    8  301.0  335  3.54  3.570  14.60   0   1     5
# Volvo 142E           21.4    4  121.0  109  4.11  2.780  18.60   1   1     4

#                      carb  row_num
# Mazda RX4               4     True
# Mazda RX4 Wag           4    False
# Datsun 710              1    False
# Hornet 4 Drive          1    False
# Hornet Sportabout       2    False
# Valiant                 1    False
# Duster 360              4    False
# Merc 240D               2    False
# Merc 230                2    False
# Merc 280                4    False
# Merc 280C               4    False
# Merc 450SE              3    False
# Merc 450SL              3    False
# Merc 450SLC             3    False
# Cadillac Fleetwood      4    False
# Lincoln Continental     4    False
# Chrysler Imperial       4    False
# Fiat 128                1    False
# Honda Civic             2    False
# Toyota Corolla          1    False
# Toyota Corona           1    False
# Dodge Challenger        2    False
# AMC Javelin             2    False
# Camaro Z28              4    False
# Pontiac Firebird        2    False
# Fiat X1-9               1    False
# Porsche 914-2           2    False
# Lotus Europa            2    False
# Ford Pantera L          4    False
# Ferrari Dino            6    False
# Maserati Bora           8    False
# Volvo 142E              2    False
```

</td>
<td>

```R
# row_number can be used with single table verbs without specifying x
# (for data frames and databases that support windowing)
mutate(mtcars, row_number() == 1L)
#>     mpg cyl  disp  hp drat    wt  qsec vs am gear carb row_number() == 1L
#> 1  21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4               TRUE
#> 2  21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4              FALSE
#> 3  22.8   4 108.0  93 3.85 2.320 18.61  1  1    4    1              FALSE
#> 4  21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1              FALSE
#> 5  18.7   8 360.0 175 3.15 3.440 17.02  0  0    3    2              FALSE
#> 6  18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1              FALSE
#> 7  14.3   8 360.0 245 3.21 3.570 15.84  0  0    3    4              FALSE
#> 8  24.4   4 146.7  62 3.69 3.190 20.00  1  0    4    2              FALSE
#> 9  22.8   4 140.8  95 3.92 3.150 22.90  1  0    4    2              FALSE
#> 10 19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4              FALSE
#> 11 17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4              FALSE
#> 12 16.4   8 275.8 180 3.07 4.070 17.40  0  0    3    3              FALSE
#> 13 17.3   8 275.8 180 3.07 3.730 17.60  0  0    3    3              FALSE
#> 14 15.2   8 275.8 180 3.07 3.780 18.00  0  0    3    3              FALSE
#> 15 10.4   8 472.0 205 2.93 5.250 17.98  0  0    3    4              FALSE
#> 16 10.4   8 460.0 215 3.00 5.424 17.82  0  0    3    4              FALSE
#> 17 14.7   8 440.0 230 3.23 5.345 17.42  0  0    3    4              FALSE
#> 18 32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1              FALSE
#> 19 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2              FALSE
#> 20 33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1              FALSE
#> 21 21.5   4 120.1  97 3.70 2.465 20.01  1  0    3    1              FALSE
#> 22 15.5   8 318.0 150 2.76 3.520 16.87  0  0    3    2              FALSE
#> 23 15.2   8 304.0 150 3.15 3.435 17.30  0  0    3    2              FALSE
#> 24 13.3   8 350.0 245 3.73 3.840 15.41  0  0    3    4              FALSE
#> 25 19.2   8 400.0 175 3.08 3.845 17.05  0  0    3    2              FALSE
#> 26 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1              FALSE
#> 27 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2              FALSE
#> 28 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2              FALSE
#> 29 15.8   8 351.0 264 4.22 3.170 14.50  0  1    5    4              FALSE
#> 30 19.7   6 145.0 175 3.62 2.770 15.50  0  1    5    6              FALSE
#> 31 15.0   8 301.0 335 3.54 3.570 14.60  0  1    5    8              FALSE
#> 32 21.4   4 121.0 109 4.11 2.780 18.60  1  1    4    2              FALSE
```

</td>
</tr>
<tr valign="top">
<td>

```python
# between has an additional argument inclusive
# if True (default): check 0 <= x <= 9
# otherwise: 0 <= x < 9
mtcars >> filter(between(row_number(), 0, 9))
#                     mpg  cyl   disp   hp  drat     wt   qsec  vs  am  gear  \
# Mazda RX4          21.0    6  160.0  110  3.90  2.620  16.46   0   1     4
# Mazda RX4 Wag      21.0    6  160.0  110  3.90  2.875  17.02   0   1     4
# Datsun 710         22.8    4  108.0   93  3.85  2.320  18.61   1   1     4
# Hornet 4 Drive     21.4    6  258.0  110  3.08  3.215  19.44   1   0     3
# Hornet Sportabout  18.7    8  360.0  175  3.15  3.440  17.02   0   0     3
# Valiant            18.1    6  225.0  105  2.76  3.460  20.22   1   0     3
# Duster 360         14.3    8  360.0  245  3.21  3.570  15.84   0   0     3
# Merc 240D          24.4    4  146.7   62  3.69  3.190  20.00   1   0     4
# Merc 230           22.8    4  140.8   95  3.92  3.150  22.90   1   0     4
# Merc 280           19.2    6  167.6  123  3.92  3.440  18.30   1   0     4

#                    carb
# Mazda RX4             4
# Mazda RX4 Wag         4
# Datsun 710            1
# Hornet 4 Drive        1
# Hornet Sportabout     2
# Valiant               1
# Duster 360            4
# Merc 240D             2
# Merc 230              2
# Merc 280              4
```

</td>
<td>

```R
mtcars %>% filter(between(row_number(), 1, 10))
#>                    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> Mazda RX4         21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4
#> Mazda RX4 Wag     21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4
#> Datsun 710        22.8   4 108.0  93 3.85 2.320 18.61  1  1    4    1
#> Hornet 4 Drive    21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1
#> Hornet Sportabout 18.7   8 360.0 175 3.15 3.440 17.02  0  0    3    2
#> Valiant           18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1
#> Duster 360        14.3   8 360.0 245 3.21 3.570 15.84  0  0    3    4
#> Merc 240D         24.4   4 146.7  62 3.69 3.190 20.00  1  0    4    2
#> Merc 230          22.8   4 140.8  95 3.92 3.150 22.90  1  0    4    2
#> Merc 280          19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4
```

</td>
</tr>
</table>
