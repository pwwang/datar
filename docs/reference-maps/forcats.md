<style>
.md-typeset__table {
   min-width: 100%;
}

.md-typeset table:not([class]) {
    display: table;
    max-width: 80%;
}
</style>

## Reference of `datar.forcats`

Reference map of `r-tidyverse-forcats` can be found [here][1].

<u>**Legend:**</u>

|Sample|Status|
|---|---|
|[normal]()|API that is regularly ported|
|<s>[strike-through]()</s>|API that is not ported, or not an API originally|
|[**bold**]()|API that is unique in `datar`|
|[_italic_]()|Working in process|

### Change order of levels

|API|Description|Notebook example|
|---|---|---:|
|[fct_relevel()][2]|Reorder factor levels by hand|[:material-notebook:][3]|
|[fct_inorder()][4] [fct_infreq()][5] [fct_inseq()][6]|Reorder factor levels by first appearance, frequency, or numeric order|[:material-notebook:][3]|
|[fct_reorder()][7] [fct_reorder2()][8] [last2()][9] [first2()][10]|Reorder factor levels by sorting along another variable|[:material-notebook:][3]|
|[fct_shuffle()][11]|Randomly permute factor levels|[:material-notebook:][3]|
|[fct_rev()][12]|Reverse order of factor levels|[:material-notebook:][3]|
|[fct_shift()][13]|Shift factor levels to left or right, wrapping around at end|[:material-notebook:][3]|

### Change value of levels

|API|Description|Notebook example|
|---|---|---:|
|[fct_anon()][15]|Anonymise factor levels|[:material-notebook:][14]|
|[fct_collapse()][16]|Collapse factor levels into manually defined groups|[:material-notebook:][14]|
|[fct_lump()][17] [fct_lump_min()][18] [fct_lump_prop()][19] [fct_lump_n()][20] [fct_lump_lowfreq()][41]|Lump together actor levels into "other"|[:material-notebook:][14]|
|[fct_other()][21]|Replace levels with "other"|[:material-notebook:][14]|
|[fct_recode()][22]|Change factor levels by hand|[:material-notebook:][14]|
|[fct_relabel()][23]|Automatically relabel factor levels, collapse as necessary|[:material-notebook:][14]|

### Add/remove levels

|API|Description|Notebook example|
|---|---|---:|
|[fct_expand()][25]|Add additional levels to a factor|[:material-notebook:][24]|
|[fct_explicit_na()][26]|Make missing values explicit||[:material-notebook:][24]|
|[fct_drop()][27]|Drop unused levels||[:material-notebook:][24]|
|[fct_unify()][28]|Unify the levels in a list of factors||[:material-notebook:][24]|

### Combine multiple factors

|API|Description|Notebook example|
|---|---|---:|
|[fct_c()][29]|Concatenate factors, combining levels|[:material-notebook:][31]|
|[fct_cross()][30]|Combine levels from two or more factors to create a new factor|[:material-notebook:][31]|

### Other helpers

|API|Description|Notebook example|
|---|---|---:|
|[as_factor()][33]|Convert input to a factor|[:material-notebook:][32]|
|[fct_count()][34]|Count entries in a factor|[:material-notebook:][32]|
|[fct_match()][35]|Test for presence of levels in a factor|[:material-notebook:][32]|
|[fct_unique()][36]|Unique values of a factor|[:material-notebook:][32]|
|[lvls_reorder()][37] [lvls_revalue()][38] [lvls_expand()][39]|Low-level functions for manipulating levels|[:material-notebook:][32]|
|[lvls_union()][40]|Find all levels in a list of factors|[:material-notebook:][32]|

[1]: https://forcats.tidyverse.org/reference/index.html
[2]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_relevel
[3]: ../../notebooks/forcats_lvl_order
[4]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_inorder
[5]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_infreq
[6]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_inseq
[7]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_reorder
[8]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_reorder2
[9]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.last2
[10]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.first2
[11]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_shuffle
[12]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_rev
[13]: ../../api/datar.forcats.lvl_order/#datar.tidyr.lvl_order.fct_shift
[14]: ../../notebooks/forcats_lvl_value
[15]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_relevel
[16]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_relevel
[17]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_lump
[18]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_lump_min
[19]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_lump_prop
[20]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_lump_n
[21]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_other
[22]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_recode
[23]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_relabel
[24]: ../../notebooks/forcats_lvl_addrm
[25]: ../../api/datar.forcats.lvl_addrm/#datar.tidyr.lvl_addrm.fct_expand
[26]: ../../api/datar.forcats.lvl_addrm/#datar.tidyr.lvl_addrm.fct_explicit_na
[27]: ../../api/datar.forcats.lvl_addrm/#datar.tidyr.lvl_addrm.fct_drop
[28]: ../../api/datar.forcats.lvl_addrm/#datar.tidyr.lvl_addrm.fct_unify
[29]: ../../api/datar.forcats.fct_multi/#datar.tidyr.fct_multi.fct_c
[30]: ../../api/datar.forcats.fct_multi/#datar.tidyr.fct_multi.fct_cross
[31]: ../../notebooks/forcats_fct_multi
[32]: ../../notebooks/forcats_misc
[33]: ../../api/datar.forcats.misc/#datar.tidyr.misc.as_factor
[34]: ../../api/datar.forcats.misc/#datar.tidyr.misc.fct_count
[35]: ../../api/datar.forcats.misc/#datar.tidyr.misc.fct_match
[36]: ../../api/datar.forcats.misc/#datar.tidyr.misc.fct_unique
[37]: ../../api/datar.forcats.misc/#datar.tidyr.misc.lvls_reorder
[38]: ../../api/datar.forcats.misc/#datar.tidyr.misc.lvls_revalue
[39]: ../../api/datar.forcats.misc/#datar.tidyr.misc.lvls_expand
[40]: ../../api/datar.forcats.misc/#datar.tidyr.misc.lvls_union
[41]: ../../api/datar.forcats.lvl_value/#datar.tidyr.lvl_value.fct_lump_lowfreq
