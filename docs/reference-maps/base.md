<style>
.md-typeset__table {
   min-width: 100%;
}

.md-typeset table:not([class]) {
    display: table;
    max-width: 80%;
}
</style>

## Reference of `datar.base`

<u>**Legend:**</u>

|Sample|Status|
|---|---|
|[normal]()|API that is regularly ported|
|<s>[strike-through]()</s>|API that is not ported, or not an API originally|
|[**bold**]()|API that is unique in `datar`|
|[_italic_]()|Working in process|

### Constants

|API|Description|Notebook example|
|---|---|---:|
|`pi`|the ratio of the circumference of a circle to its diameter.|[:material-notebook:][4]|
|`letters`|the 26 lower-case letters of the Roman alphabet|[:material-notebook:][4]|
|`LETTERS`|the 26 upper-case letters of the Roman alphabet|[:material-notebook:][4]|
|`month.abb`|the three-letter abbreviations for the English month names|[:material-notebook:][4]|
|`month.name`|the English names for the months of the year|[:material-notebook:][4]|

### Options

|API|Description|Notebook example|
|---|---|---:|
|[`options`][5]|Allow the user to set and examine a variety of global _options_|[:material-notebook:][4]|
|[`get_option()`][6]|Get the value of a certain option (R's `getOption()`)|[:material-notebook:][4]|
|[**`options_context()`**][7]|A context manager to temporarily modify the options|[:material-notebook:][4]|

### Arithmetic functions
|API|Description|Notebook example|
|---|---|---:|
|[`mean()`][8]|Calculate the mean of the values|[:material-notebook:][4]|
|[`median()`][9]|Calculate the median of the values|[:material-notebook:][4]|
|[`min()`][10]|Calculate the min of the values|[:material-notebook:][4]|
|[`max()`][11]|Calculate the max of the values|[:material-notebook:][4]|
|[`pmin()`][12]|Calculate the min of the values rowwisely|[:material-notebook:][4]|
|[`pmax()`][13]|Calculate the max of the values rowwisely|[:material-notebook:][4]|
|[`sum()`][14]|Calculate the sum of the values|[:material-notebook:][4]|
|[`abs()`][15]|Calculate the absolute values of the values|[:material-notebook:][4]|
|[`round()`][16]|Round the numbers|[:material-notebook:][4]|
|[`var()`][17]|Calculate the variance of the values|[:material-notebook:][4]|
|[`ceiling()`][18]|Get the ceiling integers of the numbers|[:material-notebook:][4]|
|[`floor()`][19]|Get the floor integers of the numbers|[:material-notebook:][4]|
|[`sqrt()`][20]|Get the square root of the numbers|[:material-notebook:][4]|
|[`cov()`][21]|Calculate the covariance of the values|[:material-notebook:][4]|
|[`prod()`][117]|Calculate Product of the input||
|[`sign()`][118]|Get the signs of the corresponding elements of x||
|[`signif()`][125]|Rounds the values in its first argument to the specified number of significant digits||
|[`trunc()`][119]|Get the integers truncated for each element in x||
|[`exp()`][120]|Calculates the power of natural number||
|[`log()`][121]|Computes logarithms, by default natural logarithm||
|[`log2()`][122]|Computes logarithms with base 2||
|[`log10()`][123]|Computes logarithms with base 10||
|[`log1p()`][124]|Computes log(1+x)||

### Bessel functions

|API|Description|Notebook example|
|---|---|---:|
|[`bessel_i`][22]|Bessel Functions of integer and fractional order of first kind|[:material-notebook:][4]|
|[`bessel_k`][24]|Bessel Functions of integer and fractional order of second kind|[:material-notebook:][4]|
|[`bessel_j`][23]|Modified Bessel functions of first kind|[:material-notebook:][4]|
|[`bessel_y`][25]|Modified Bessel functions of third kind|[:material-notebook:][4]|

### Casting values between types

|API|Description|Notebook example|
|---|---|---:|
|[`as_integer`][26] [`as_int`][]|Cast data to integer|[:material-notebook:][4]|
|[`as_double`][27]|Cast data to double (`numpy.float64`)|[:material-notebook:][4]|
|[`as_float`][28]|Cast data to float (`numpy.float_`)|[:material-notebook:][4]|
|[`as_numeric`][29]|Cast data to numeric|[:material-notebook:][4]|

### Complex numbers

|API|Description|Notebook example|
|---|---|---:|
|[`re`][30]|Get the real part of a complex number|[:material-notebook:][4]|
|[`mod`][31]|Get the modulus of a complex number|[:material-notebook:][4]|
|[`im`][32]|Get the imaginary part of a complex number|[:material-notebook:][4]|
|[`arg`][33]|Get the argument of a complex number|[:material-notebook:][4]|
|[`conj`][34]|Get the complex conjugate of a complex number|[:material-notebook:][4]|
|[`is_complex`][35]|Test if data is complex number|[:material-notebook:][4]|
|[`as_complex`][36]|Cast data to a complex number|[:material-notebook:][4]|

### Cumulativate functions

|API|Description|Notebook example|
|---|---|---:|
|[`cumsum`][37]|Cummulative sum|[:material-notebook:][4]|
|[`cumprod`][38]|Cummulative product|[:material-notebook:][4]|
|[`cummin`][39]|Cummulative min|[:material-notebook:][4]|
|[`cummax`][40]|Cummulative max|[:material-notebook:][4]|

### Date functions

|API|Description|Notebook example|
|---|---|---:|
|[`as_date`][41]|Cast data to date|[:material-notebook:][4]|

### Factor data

|API|Description|Notebook example|
|---|---|---:|
|[`factor`][42]|Construct factor|[:material-notebook:][4]|
|[`droplevels`][43]|Drop unused levels|[:material-notebook:][4]|
|[`levels`][44]|Get levels of factors|[:material-notebook:][4]|
|[`is_factor`][45] [`is_categorical`][45]|Test if data is factor|[:material-notebook:][4]|
|[`as_factor`][46] [`as_categorical`][46]|Cast data to factor|[:material-notebook:][4]|

### Logical/Boolean values

|API|Description|Notebook example|
|---|---|---:|
|`TRUE`|Logical true|[:material-notebook:][4]|
|`FALSE`|Logical false|[:material-notebook:][4]|
|[`is_true`][47]|Test if data is scalar true (R's `isTRUE`)|[:material-notebook:][4]|
|[`is_false`][48]|Test if data is scalar false (R's `FALSE`)|[:material-notebook:][4]|
|[`is_logical`][49] [`is_bool`][49]|Test if data is logical/boolean|[:material-notebook:][4]|
|[`as_logical`][50] [`as_bool`][50]|Cast data to logical/boolean|[:material-notebook:][4]|

### NA (missing values)

|API|Description|Notebook example|
|---|---|---:|
|`Inf`|Infinite number|[:material-notebook:][4]|
|`NA`|Missing value|[:material-notebook:][4]|
|`NaN`|Missing value, same as `NA`|[:material-notebook:][4]|
|[`is_na()`][51]|Test if data is NA|[:material-notebook:][4]|
|[`any_na()`][52]|Test if any element is NA|[:material-notebook:][4]|
|[`is_finite`][126]|Test if x is finite||
|[`is_infinite`][127]|Test if x is infinite||
|[`is_nan`][128]|Test if x is nan||

### NULL

|API|Description|Notebook example|
|---|---|---:|
|`NULL`|NULL value|[:material-notebook:][4]|
|[`is_null`][53]|Test if data is null|[:material-notebook:][4]|
|[`as_null`][54]|Cast anything to NULL|[:material-notebook:][4]|

### Random

|API|Description|Notebook example|
|---|---|---:|
|[`set_seed`][55]|Set the randomization seed|[:material-notebook:][4]|

### Functions to create and manipulate sequences

|API|Description|Notebook example|
|---|---|---:|
|[`c`][56]|Collection of data|[:material-notebook:][4]|
|[`seq`][57]|Generate sequence|[:material-notebook:][4]|
|[`seq_len`][58]|Generate sequence with length|[:material-notebook:][4]|
|[`seq_along`][59]|Generate sequence along with another sequence|[:material-notebook:][4]|
|[`rev`][60]|Reverse a sequence|[:material-notebook:][4]|
|[`rep`][61]|Generate sequence with repeats|[:material-notebook:][4]|
|[`lengths`][62]|Get the length of elements in the sequence|[:material-notebook:][4]|
|[`unique`][63]|Get the unique elements|[:material-notebook:][4]|
|[`sample`][64]|Sample the elements from sequence|[:material-notebook:][4]|
|[`length`][65]|Get the length of data|[:material-notebook:][4]|
|[`match`][129]|match returns a vector of the positions of (first) matches of its first argument in its second.||

### Special functions

|API|Description|Notebook example|
|---|---|---:|
|[`beta`][66]|Beta function|[:material-notebook:][4]|
|[`lbeta`][67]|Natural logarithm of beta function|[:material-notebook:][4]|
|[`gamma`][68]|Gamma function|[:material-notebook:][4]|
|[`lgamma`][69]|Natural logarithm of gamma function|[:material-notebook:][4]|
|[`digamma`][70]|the first derivatives of the logarithm of the gamma function.|[:material-notebook:][4]|
|[`trigamma`][71]|the second derivatives of the logarithm of the gamma function.|[:material-notebook:][4]|
|[`psigamma`][72]|polygamma funnction|[:material-notebook:][4]|
|[`choose`][73]|binomial coefficients|[:material-notebook:][4]|
|[`lchoose`][74]|the logarithms of binomial coefficients.|[:material-notebook:][4]|
|[`factorial`][75]|factorial|[:material-notebook:][4]|
|[`lfactorial`][76]|Natural logarithm of factorial|[:material-notebook:][4]|

### String functions

|API|Description|Notebook example|
|---|---|---:|
|[`is_character`][77] [`is_str`][77] [`is_string`][77]|Test if data is string|[:material-notebook:][4]|
|[`as_character`][78] [`as_str`][78] [`as_string`][78]|Cast data to string|[:material-notebook:][4]|
|[`grep`][79]|Test if pattern in string|[:material-notebook:][4]|
|[`grepl`][80]|Logical version of `grep`|[:material-notebook:][4]|
|[`sub`][81]|Replace substrings in strings|[:material-notebook:][4]|
|[`gsub`][82]|Replace all matched substring in strings|[:material-notebook:][4]|
|[`nchar`][83]|Get length of string|[:material-notebook:][4]|
|[`nzhcar`][84]|Test if string is not empty|[:material-notebook:][4]|
|[`paste`][85]|Concatenate strings|[:material-notebook:][4]|
|[`paste0`][86]|Concatenate strings with `sep=''`|[:material-notebook:][4]|
|[`sprintf`][87]|C-style string formatting|[:material-notebook:][4]|
|[`substr`][88]|Get substring|[:material-notebook:][4]|
|[`substring`][89]|Get substring with a start only|[:material-notebook:][4]|
|[`strsplit`][90]|Split strings with delimiter|[:material-notebook:][4]|
|[`startswith`][130]|Test if strings start with given prefix||
|[`endswith`][131]|Test if strings end with given suffix||
|[`strtoi`][132]|Convert strings to integers||
|[`chartr`][133]|Replace characters in strings||
|[`tolower`][134]|Transform strings to lower case||
|[`toupper`][135]|Transform strings to upper case||

### Table

|API|Description|Notebook example|
|---|---|---:|
|[`table`][91]|Cross Tabulation and Table Creation|[:material-notebook:][4]|

### Testing value types

|API|Description|Notebook example|
|---|---|---:|
|[`is_double`][92] [`is_float`][92]|Test if data is double or float (`numpy.float_`)|[:material-notebook:][4]|
|[`is_integer`][93] [`is_int`][93]|Test if data is integer|[:material-notebook:][4]|
|[`is_numeric`][94]|Test if data is numeric|[:material-notebook:][4]|
|[`is_atomic`][95]|Test is data is atomic|[:material-notebook:][4]|
|[`is_element`][96] [`is_in`][96]|Test if value is an element of an array (R's `%in`)|[:material-notebook:][4]|

### Trigonometric and hyper bolic functions

|API|Description|Notebook example|
|---|---|---:|
|[`cos`][97]|cosine|[:material-notebook:][4]|
|[`sin`][98]|sine|[:material-notebook:][4]|
|[`tan`][99]|tangent|[:material-notebook:][4]|
|[`acos`][100]|Arc-cosine|[:material-notebook:][4]|
|[`asin`][101]|Arc-sine|[:material-notebook:][4]|
|[`atan`][102]|Arc-tangent|[:material-notebook:][4]|
|[`atan2`][103]|`atan(y/x)`|[:material-notebook:][4]|
|[`cospi`][104]|`cos(pi*x)`|[:material-notebook:][4]|
|[`sinpi`][105]|`sin(pi*x)`|[:material-notebook:][4]|
|[`tanpi`][106]|`tan(pi*x)`|[:material-notebook:][4]|
|[`cosh`][107]|Hyperbolic cosine|[:material-notebook:][4]|
|[`sinh`][108]|Hyperbolic sine|[:material-notebook:][4]|
|[`tanh`][109]|Hyperbolic tangent|[:material-notebook:][4]|
|[`acosh`][110]|Hyperbolic cosine|[:material-notebook:][4]|
|[`asinh`][111]|Hyperbolic sine|[:material-notebook:][4]|
|[`atanh`][112]|Hyperbolic tangent|[:material-notebook:][4]|

### Which

|API|Description|Notebook example|
|---|---|---:|
|[which()][1]|Which indices are True?|[:material-notebook:][4]|
|[which_min()][2] [which_max()][3]|Where is the minimum or maximum or first TRUE or FALSE|[:material-notebook:][4]|

### Other functions

|API|Description|Notebook example|
|---|---|---:|
|[`cut`][113]|Convert Numeric to Factor|[:material-notebook:][4]|
|[`identity`][114]|Identity Function|[:material-notebook:][4]|
|[`expandgrid`][115]|Create a Data Frame from All Combinations of Factor Variables|[:material-notebook:][4]|
|[`max_col`][136]|Find the maximum position for each row of a matrix||
|[`complete_cases`][137]|Get a bool array indicating whether the values of rows are complete in a data frame.||
|[**`data_context`**][116]|Mimic R's `with`|[:material-notebook:][4]|


[1]: ../../api/datar.base.which/#datar.dplyr.which.which
[2]: ../../api/datar.base.which/#datar.dplyr.which.which_min
[3]: ../../api/datar.base.which/#datar.dplyr.which.which_max
[4]: ../../notebooks/base
[5]: ../../api/datar.core.options/#datar.core.options.options
[6]: ../../api/datar.core.options/#datar.core.options.get_option
[7]: ../../api/datar.core.options/#datar.core.options.options_context
[8]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.mean
[9]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.median
[10]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.min
[11]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.max
[12]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.pmin
[13]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.pmax
[14]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.sum
[15]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.abs
[16]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.round
[17]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.var
[18]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.ceiling
[19]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.floor
[20]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.sqrt
[21]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.cov
[22]: ../../api/datar.core.bessel/#datar.core.bessel.bessel_i
[23]: ../../api/datar.core.bessel/#datar.core.bessel.bessel_j
[24]: ../../api/datar.core.bessel/#datar.core.bessel.bessel_k
[25]: ../../api/datar.core.bessel/#datar.core.bessel.bessel_y
[26]: ../../api/datar.core.casting/#datar.core.casting.as_integer
[27]: ../../api/datar.core.casting/#datar.core.casting.as_double
[28]: ../../api/datar.core.casting/#datar.core.casting.as_float
[29]: ../../api/datar.core.casting/#datar.core.casting.as_numeric
[30]: ../../api/datar.core.complex/#datar.core.complex.re
[31]: ../../api/datar.core.complex/#datar.core.complex.mod
[32]: ../../api/datar.core.complex/#datar.core.complex.im
[33]: ../../api/datar.core.complex/#datar.core.complex.arg
[34]: ../../api/datar.core.complex/#datar.core.complex.conj
[35]: ../../api/datar.core.complex/#datar.core.complex.is_complex
[36]: ../../api/datar.core.complex/#datar.core.complex.as_complex
[37]: ../../api/datar.core.cum/#datar.core.cum.cumsum
[38]: ../../api/datar.core.cum/#datar.core.cum.cumprod
[39]: ../../api/datar.core.cum/#datar.core.cum.cummin
[40]: ../../api/datar.core.cum/#datar.core.cum.cummax
[41]: ../../api/datar.core.date/#datar.core.date.as_date
[42]: ../../api/datar.core.factor/#datar.core.factor.factor
[43]: ../../api/datar.core.factor/#datar.core.factor.droplevels
[44]: ../../api/datar.core.factor/#datar.core.factor.levels
[45]: ../../api/datar.core.factor/#datar.core.factor.is_factor
[46]: ../../api/datar.core.factor/#datar.core.factor.as_factor
[47]: ../../api/datar.core.logical/#datar.core.logical.is_true
[48]: ../../api/datar.core.logical/#datar.core.logical.is_false
[49]: ../../api/datar.core.logical/#datar.core.logical.is_logical
[50]: ../../api/datar.core.logical/#datar.core.logical.as_logical
[51]: ../../api/datar.core.na/#datar.core.na.is_na
[52]: ../../api/datar.core.na/#datar.core.na.any_na
[53]: ../../api/datar.core.null/#datar.core.null.is_null
[54]: ../../api/datar.core.null/#datar.core.null.as_null
[55]: ../../api/datar.core.random/#datar.core.random.set_seed
[56]: ../../api/datar.core.seq/#datar.core.seq.c
[57]: ../../api/datar.core.seq/#datar.core.seq.seq
[58]: ../../api/datar.core.seq/#datar.core.seq.seq_len
[59]: ../../api/datar.core.seq/#datar.core.seq.seq_along
[60]: ../../api/datar.core.seq/#datar.core.seq.rev
[61]: ../../api/datar.core.seq/#datar.core.seq.rep
[62]: ../../api/datar.core.seq/#datar.core.seq.lengths
[63]: ../../api/datar.core.seq/#datar.core.seq.unique
[64]: ../../api/datar.core.seq/#datar.core.seq.sample
[65]: ../../api/datar.core.seq/#datar.core.seq.length
[66]: ../../api/datar.core.special/#datar.core.special.beta
[67]: ../../api/datar.core.special/#datar.core.special.lbeta
[68]: ../../api/datar.core.special/#datar.core.special.gamma
[69]: ../../api/datar.core.special/#datar.core.special.lgamma
[70]: ../../api/datar.core.special/#datar.core.special.digamma
[71]: ../../api/datar.core.special/#datar.core.special.trigamma
[72]: ../../api/datar.core.special/#datar.core.special.psigamma
[73]: ../../api/datar.core.special/#datar.core.special.choose
[74]: ../../api/datar.core.special/#datar.core.special.lchoose
[75]: ../../api/datar.core.special/#datar.core.special.factorial
[76]: ../../api/datar.core.special/#datar.core.special.lfactorial
[77]: ../../api/datar.core.string/#datar.core.string.is_character
[78]: ../../api/datar.core.string/#datar.core.string.as_character
[79]: ../../api/datar.core.string/#datar.core.string.grep
[80]: ../../api/datar.core.string/#datar.core.string.grepl
[81]: ../../api/datar.core.string/#datar.core.string.sub
[82]: ../../api/datar.core.string/#datar.core.string.gsub
[83]: ../../api/datar.core.string/#datar.core.string.nchar
[84]: ../../api/datar.core.string/#datar.core.string.nzchar
[85]: ../../api/datar.core.string/#datar.core.string.paste
[86]: ../../api/datar.core.string/#datar.core.string.paste0
[87]: ../../api/datar.core.string/#datar.core.string.sprintf
[88]: ../../api/datar.core.string/#datar.core.string.substr
[89]: ../../api/datar.core.string/#datar.core.string.substring
[90]: ../../api/datar.core.string/#datar.core.string.strsplit
[91]: ../../api/datar.core.table/#datar.core.table.table
[92]: ../../api/datar.core.testing/#datar.core.testing.is_double
[93]: ../../api/datar.core.testing/#datar.core.testing.is_float
[94]: ../../api/datar.core.testing/#datar.core.testing.is_integer
[95]: ../../api/datar.core.testing/#datar.core.testing.is_atomic
[96]: ../../api/datar.core.testing/#datar.core.testing.is_element
[97]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.cos
[98]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.sin
[99]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.tan
[100]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.acos
[101]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.asin
[102]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.atan
[103]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.atan2
[104]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.cospi
[105]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.sinpi
[106]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.tanpi
[107]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.cosh
[108]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.sinh
[109]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.tanh
[110]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.acosh
[111]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.asinh
[112]: ../../api/datar.core.trig_hb/#datar.core.trig_hb.atanh
[113]: ../../api/datar.core.funs/#datar.core.funs.cut
[114]: ../../api/datar.core.funs/#datar.core.funs.identity
[115]: ../../api/datar.core.funs/#datar.core.funs.expandgrid
[116]: ../../api/datar.core.funs/#datar.core.funs.data_context
[117]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.prod
[118]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.sign
[119]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.trunc
[120]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.exp
[121]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.log
[122]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.log2
[123]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.log10
[124]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.log1p
[125]: ../../api/datar.core.arithmetic/#datar.core.arithmetic.signif
[126]: ../../api/datar.core.na/#datar.core.na.is_finite
[127]: ../../api/datar.core.na/#datar.core.na.is_infinite
[128]: ../../api/datar.core.na/#datar.core.na.is_nan
[129]: ../../api/datar.core.seq/#datar.core.seq.match
[130]: ../../api/datar.core.string/#datar.core.string.startswith
[131]: ../../api/datar.core.string/#datar.core.string.endswith
[132]: ../../api/datar.core.string/#datar.core.string.strtoi
[133]: ../../api/datar.core.string/#datar.core.string.chartr
[134]: ../../api/datar.core.string/#datar.core.string.tolower
[135]: ../../api/datar.core.string/#datar.core.string.toupper
[136]: ../../api/datar.core.verbs/#datar.core.verbs.max_col
[137]: ../../api/datar.core.verbs/#datar.core.verbs.complete_cases
