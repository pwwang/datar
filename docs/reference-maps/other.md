<style>
.md-typeset__table {
   min-width: 100%;
}

.md-typeset table:not([class]) {
    display: table;
    max-width: 80%;
}
</style>

## Reference of `datar.datar`

<u>**Legend:**</u>

|Sample|Status|
|---|---|
|[normal]()|API that is regularly ported|
|<s>[strike-through]()</s>|API that is not ported, or not an API originally|
|[**bold**]()|API that is unique in `datar`|
|[_italic_]()|Working in process|

### Verbs

|API|Description|Notebook example|
|---|---|---:|
|[**`get()`**][2]|Extract values from data frames|[:material-notebook:][1]|
|[**`flatten()`**][2]|Flatten values of data frames|[:material-notebook:][1]|

### Functions

|[**`itemgetter()`**][3]|Turn `a[f.x]` to a valid verb argument with `itemgetter(a, f.x)`|[:material-notebook:][1]|
|[**`attrgetter()`**][4]|`f.x.<attr>` but works with `SeriesGroupBy` object|[:material-notebook:][1]|
|[**`pd_str()`**][4]|`str` accessor but works with `SeriesGroupBy` object|[:material-notebook:][1]|
|[**`pd_cat()`**][4]|`cat` accessor but works with `SeriesGroupBy` object|[:material-notebook:][1]|
|[**`pd_dt()`**][4]|`dt` accessor but works with `SeriesGroupBy` object|[:material-notebook:][1]|


[1]: ../../notebooks/datar
[2]: ../../api/datar.datar.verbs/#datar.datar.verbs.get
[3]: ../../api/datar.datar.verbs/#datar.datar.verbs.flatten
[4]: ../../api/datar.datar.funcs/#datar.datar.funcs.itemgetter
[5]: ../../api/datar.datar.funcs/#datar.datar.funcs.attrgetter
[6]: ../../api/datar.datar.funcs/#datar.datar.funcs.pd_str
[7]: ../../api/datar.datar.funcs/#datar.datar.funcs.pd_cat
[8]: ../../api/datar.datar.funcs/#datar.datar.funcs.pd_dt
