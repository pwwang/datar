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
|pivot_longer()|Pivot data from wide to long||
|pivot_wider()|Pivot data from long to wide||
|spread()|Spread a key-value pair across multiple columns||
|gather()|Gather columns into key-value pairs||

### Rectangling

|API|Description|Notebook example|
|---|---|---:|

### Nesting

|API|Description|Notebook example|
|---|---|---:|

### Character vectors

|API|Description|Notebook example|
|---|---|---:|

### Missing values

|API|Description|Notebook example|
|---|---|---:|

### Miscellanea

|API|Description|Notebook example|
|---|---|---:|
|[`chop()`][3] [`unchop()`][4]|Chop and unchop|[:material-notebook:][5]|
|[`nest()`][9] [`unnest()`][10]|Nest and unnest|[:material-notebook:][11]|
|[`pack()`][6] [`unpack()`][7]|Pack and unpack|[:material-notebook:][8]|

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