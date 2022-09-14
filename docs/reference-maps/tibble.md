<style>
.md-typeset__table {
   min-width: 100%;
}

.md-typeset table:not([class]) {
    display: table;
    max-width: 80%;
}
</style>

## Reference of `datar.tibble`

Reference map of `r-tidyverse-tibble` can be found [here][1].

<u>**Legend:**</u>

|Sample|Status|
|---|---|
|[normal]()|API that is regularly ported|
|<s>[strike-through]()</s>|API that is not ported, or not an API originally|
|[**bold**]()|API that is unique in `datar`|
|_italic_|Working in process|


### Tibbles

!!! Tip

    Tibbles in `datar` are just `pandas.DataFrame`s. So there is no difference between data frames created by `tibble()` and `pandas.DataFrame`, unlike in R, `tibble` and `data.frame`.

    Also note that tibbles in `datar` are not `rownames`/`index` aware for most APIs, just like most `tidyverse` APIs.

|API|Description|Notebook example|
|---|---|---:|
|<s>`tibble-package`</s>|||
|[`tibble()`][12] [`tibble_row()`][18]|Build a data frame| [:material-notebook:][2] |
|[`fibble()`][13]|Same as `tibble()` but used as Verb arguments| [:material-notebook:][2] |
|<s>`tbl_df-class`</s>|||
|<s>`print(<tbl_df>)`</s> <s>`format(<tbl_df>)`</s>|||
|[`tribble()`][3]|Row-wise tibble creation|[:material-notebook:][2]|

### Coercion

|API|Description|Notebook example|
|---|---|---:|
|<s>`is_tibble()`</s>|||
|[`as_tibble()`][19]|Convert data frames into datar's tibbles||
|<s>`new_tibble()`</s> <s>`validate_tibble()`</s>|||
|[`enframe()`][4] [`deframe()`][14]|Converting iterables to data frames, and vice versa| [:material-notebook:][5]|

### Manipulation

|API|Description|Notebook example|
|---|---|---:|
|<s>`$` `[[` `[`</s>|Please subset data frames using `pandas` syntax (`df.col`, `df['col']`, `df.loc[...]` or `df.iloc[...]`|
|[`add_row()`][6]| Add rows to a data frame | [:material-notebook:][7] |
|[`add_column()`][8]| Add columns to a data frame | [:material-notebook:][9] |

### Helpers

|API|Description|Notebook example|
|---|---|---:|
|<s>`reexports`</s>|||
|[`has_rownames()`/`has_index()`][10] [`remove_rownames()`/`remove_index()`/`drop_index()`][15] [`rownames_to_column()`/`index_to_column()`][16] [`rowid_to_column()` `column_to_rownames()`/`column_to_index()`][17]|Tools for working with row names/DataFrame indexes|[:material-notebook:][11]|
|<s>`view()`</s>|||

### <s>Vectors, matrices, and lists</s>


[1]: https://tibble.tidyverse.org/reference/index.html
[2]: ../../notebooks/tibble
[3]: ../../api/datar.tibble.tibble/#datar.tibble.tibble.tribble
[4]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.enframe
[5]: ../../notebooks/enframe
[6]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.add_row
[7]: ../../notebooks/add_row
[8]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.add_column
[9]: ../../notebooks/add_column
[10]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.has_rownames
[11]: ../../notebooks/rownames
[12]: ../../api/datar.tibble.tibble/#datar.tibble.tibble.tibble
[13]: ../../api/datar.tibble.tibble/#datar.tibble.tibble.fibble
[14]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.deframe
[15]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.remove_rownames
[16]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.rownames_to_column
[17]: ../../api/datar.tibble.verbs/#datar.tibble.verbs.rowid_to_column
[18]: ../../api/datar.tibble.tibble/#datar.tibble.tibble.tibble_row
[19]: ../../api/datar.tibble.tibble/#datar.tibble.tibble.as_tibble
