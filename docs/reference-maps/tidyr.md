<style>
.md-typeset__table {
   min-width: 100%;
}

.md-typeset table:not([class]) {
    display: table;
    max-width: 80%;
}
</style>

## Reference of `datar.dplyr`

Reference map of `r-tidyverse-tidyr` can be found [here][1].

<u>**Legend:**</u>

|Sample|Status|
|---|---|
|[normal]()|API that is regularly ported|
|<s>[strike-through]()</s>|API that is not ported, or not an API originally|
|[**bold**]()|API that is unique in `datar`|
|[_italic_]()|Working in process|

### Pivoting

|API|Description|Notebook example|
|---|---|---:|
|[pivot_longer()][26]|Pivot data from wide to long|[:material-notebook:][27]|
|[pivot_wider()][28]|Pivot data from long to wide|[:material-notebook:][29]|

### Rectangling

|API|Description|Notebook example|
|---|---|---:|
|_`hoist()`_ _`unnest_longer()`_ _`unnest_wider()`_ _`unnest_auto()`_|Rectangle a nested list into a tidy tibble||

### Nesting

|API|Description|Notebook example|
|---|---|---:|
|[`nest()`][9] [`unnest()`][10]|Nest and unnest|[:material-notebook:][11]|

### Character vectors

|API|Description|Notebook example|
|---|---|---:|
|[`extract()`][22]|Extract a character column into multiple columns using regular expression groups|[:material-notebook:][23]|
|[`separate()`][30]|Separate a character column into multiple columns with a regular expression or numeric locations|[:material-notebook:][31]|
|[`separate_rows()`][34]|Separate a collapsed column into multiple rows|[:material-notebook:][35]|
|[`unite()`][36]|Unite multiple columns into one by pasting strings together|[:material-notebook:][37]|

### Missing values

|API|Description|Notebook example|
|---|---|---:|
|[`complete()`][18]|Complete a data frame with missing combinations of data|[:material-notebook:][19]|
|[`drop_na()`][20]|Drop rows containing missing values|[:material-notebook:][21]|
|[`expand()`][12] [`crossing()`][13] [`nesting()`][14]|Expand data frame to include all possible combinations of values|[:material-notebook:][15]|
|[`expand_grid()`][16]|
|[`fill()`][24]|Fill in missing values with previous or next value|[:material-notebook:][25]|
|[`full_seq()`][40]|Create the full sequence of values in a vector|[:material-notebook:][41]|
|[`replace_na()`][38]|Replace NAs with specified values|[:material-notebook:][39]|

### Miscellanea

|API|Description|Notebook example|
|---|---|---:|
|[`chop()`][3] [`unchop()`][4]|Chop and unchop|[:material-notebook:][5]|
|[`pack()`][6] [`unpack()`][7]|Pack and unpack|[:material-notebook:][8]|
|[`uncount()`][32]|"Uncount" a data frame|[:material-notebook:][33]|

### Data

See [datasets][2]

[1]: https://tidyr.tidyverse.org/reference/index.html
[2]: ../datasets
[3]: ../../api/datar.tidyr.chop/#datar.tidyr.chop.chop
[4]: ../../api/datar.tidyr.chop/#datar.tidyr.chop.unchop
[5]: ../../notebooks/chop
[6]: ../../api/datar.tidyr.pack/#datar.tidyr.pack.pack
[7]: ../../api/datar.tidyr.pack/#datar.tidyr.pack.unpack
[8]: ../../notebooks/chop
[9]: ../../api/datar.tidyr.nest/#datar.tidyr.nest.nest
[10]: ../../api/datar.tidyr.nest/#datar.tidyr.nest.unnest
[11]: ../../notebooks/nest
[12]: ../../api/datar.tidyr.expand/#datar.tidyr.expand.expand
[13]: ../../api/datar.tidyr.expand/#datar.tidyr.expand.crossing
[14]: ../../api/datar.tidyr.expand/#datar.tidyr.expand.nesting
[15]: ../../notebooks/expand
[16]: ../../api/datar.tidyr.expand/#datar.tidyr.expand.expand_grid
[17]: ../../notebooks/expand_grid
[18]: ../../api/datar.tidyr.complete/#datar.tidyr.complete.complete
[19]: ../../notebooks/complete
[20]: ../../api/datar.tidyr.drop_na/#datar.tidyr.drop_na.drop_na
[21]: ../../notebooks/drop_na
[22]: ../../api/datar.tidyr.extract/#datar.tidyr.extract.extract
[23]: ../../notebooks/extract
[24]: ../../api/datar.tidyr.fill/#datar.tidyr.fill.fill
[25]: ../../notebooks/fill
[26]: ../../api/datar.tidyr.pivot_long/#datar.tidyr.pivot_long.pivot_longer
[27]: ../../notebooks/pivot_longer
[28]: ../../api/datar.tidyr.pivot_wide/#datar.tidyr.pivot_wide.pivot_wider
[29]: ../../notebooks/pivot_wider
[30]: ../../api/datar.tidyr.separate/#datar.tidyr.separate.separate
[31]: ../../notebooks/separate
[32]: ../../api/datar.tidyr.uncount/#datar.tidyr.uncount.uncount
[33]: ../../notebooks/uncount
[34]: ../../api/datar.tidyr.separate/#datar.tidyr.separate.separate_rows
[35]: ../../notebooks/separate
[36]: ../../api/datar.tidyr.unite/#datar.tidyr.unite.unite
[37]: ../../notebooks/unite
[38]: ../../api/datar.tidyr.replace_na/#datar.tidyr.replace_na.replace_na
[39]: ../../notebooks/replace_na
[40]: ../../api/datar.tidyr.funcs/#datar.tidyr.funcs.full_seq
[41]: ../../notebooks/full_seq
