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

See [here](../stats) for APIs ported from `r-stats` and [here](../utils) for APIs ported from `r-utils`

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
|[`mean()`][8]|Calculate the mean of the values|[:material-notebook:][151]|
|[`median()`][9]|Calculate the median of the values|[:material-notebook:][151]|
|[`min()`][10]|Calculate the min of the values|[:material-notebook:][151]|
|[`max()`][11]|Calculate the max of the values|[:material-notebook:][151]|
|[`pmin()`][12]|Calculate the min of the values rowwisely|[:material-notebook:][151]|
|[`pmax()`][13]|Calculate the max of the values rowwisely|[:material-notebook:][151]|
|[`sum()`][14]|Calculate the sum of the values|[:material-notebook:][151]|
|[`abs()`][15]|Calculate the absolute values of the values|[:material-notebook:][151]|
|[`round()`][16]|Round the numbers|[:material-notebook:][151]|
|[`var()`][17]|Calculate the variance of the values|[:material-notebook:][151]|
|[`ceiling()`][18]|Get the ceiling integers of the numbers|[:material-notebook:][151]|
|[`floor()`][19]|Get the floor integers of the numbers|[:material-notebook:][151]|
|[`sqrt()`][20]|Get the square root of the numbers|[:material-notebook:][151]|
|[`cov()`][21]|Calculate the covariance of the values|[:material-notebook:][151]|
|[`prod()`][117]|Calculate Product of the input|[:material-notebook:][151]|
|[`sign()`][118]|Get the signs of the corresponding elements of x|[:material-notebook:][151]|
|[`signif()`][125]|Rounds the values in its first argument to the specified number of significant digits|[:material-notebook:][151]|
|[`trunc()`][119]|Get the integers truncated for each element in x|[:material-notebook:][151]|
|[`exp()`][120]|Calculates the power of natural number|[:material-notebook:][151]|
|[`log()`][121]|Computes logarithms, by default natural logarithm|[:material-notebook:][151]|
|[`log2()`][122]|Computes logarithms with base 2|[:material-notebook:][151]|
|[`log10()`][123]|Computes logarithms with base 10|[:material-notebook:][151]|
|[`log1p()`][124]|Computes log(1+x)|[:material-notebook:][151]|
|[`quantile()`][152]|Produces sample quantiles corresponding to the given probabilities.|[:material-notebook:][151]|
|[`sd()`, `std()`][153]|Computes the standard deviation of the values|[:material-notebook:][151]|
|[`weighted_mean()`][154]|Computes the weighted mean of the values|[:material-notebook:][151]|
|[`col_sums()`][155]|Computes column sums of a dataframe|[:material-notebook:][151]|
|[`row_sums()`][156]|Computes row sums of a dataframe|[:material-notebook:][151]|
|[`col_means()`][157]|Computes column means of a dataframe|[:material-notebook:][151]|
|[`row_means()`][158]|Computes row means of a dataframe|[:material-notebook:][151]|
|[`col_sds()`][159]|Computes column sds of a dataframe|[:material-notebook:][151]|
|[`row_sds()`][160]|Computes row sds of a dataframe|[:material-notebook:][151]|
|[`col_medians()`][161]|Computes column medians of a dataframe|[:material-notebook:][151]|
|[`row_medians()`][162]|Computes row medians of a dataframe|[:material-notebook:][151]|

### Bessel functions

|API|Description|Notebook example|
|---|---|---:|
|[`bessel_i()`][22]|Bessel Functions of integer and fractional order of first kind|[:material-notebook:][4]|
|[`bessel_k()`][24]|Bessel Functions of integer and fractional order of second kind|[:material-notebook:][4]|
|[`bessel_j()`][23]|Modified Bessel functions of first kind|[:material-notebook:][4]|
|[`bessel_y()`][25]|Modified Bessel functions of third kind|[:material-notebook:][4]|

### Casting values between types

|API|Description|Notebook example|
|---|---|---:|
|[`as_integer()`][26] [`as_int`][26]|Cast data to integer|[:material-notebook:][4]|
|[`as_double()`][27]|Cast data to double (`numpy.float64`)|[:material-notebook:][4]|
|[`as_float()`][28]|Cast data to float (`numpy.float_`)|[:material-notebook:][4]|
|[`as_numeric()`][29]|Cast data to numeric|[:material-notebook:][4]|

### Complex numbers

|API|Description|Notebook example|
|---|---|---:|
|[`re()`][30]|Get the real part of a complex number|[:material-notebook:][4]|
|[`mod()`][31]|Get the modulus of a complex number|[:material-notebook:][4]|
|[`im()`][32]|Get the imaginary part of a complex number|[:material-notebook:][4]|
|[`arg()`][33]|Get the argument of a complex number|[:material-notebook:][4]|
|[`conj()`][34]|Get the complex conjugate of a complex number|[:material-notebook:][4]|
|[`is_complex()`][35]|Test if data is complex number|[:material-notebook:][4]|
|[`as_complex()`][36]|Cast data to a complex number|[:material-notebook:][4]|

### Cumulativate functions

|API|Description|Notebook example|
|---|---|---:|
|[`cumsum()`][37]|Cummulative sum|[:material-notebook:][4]|
|[`cumprod()`][38]|Cummulative product|[:material-notebook:][4]|
|[`cummin()`][39]|Cummulative min|[:material-notebook:][4]|
|[`cummax()`][40]|Cummulative max|[:material-notebook:][4]|

### Date functions

|API|Description|Notebook example|
|---|---|---:|
|[`as_date()`][41]|Cast data to date|[:material-notebook:][4]|
|[**`as_pd_date()`**][150]|Alias of `pandas.to_datetime()`||

### Factor data

|API|Description|Notebook example|
|---|---|---:|
|[`factor()`][42]|Construct factor|[:material-notebook:][4]|
|[`droplevels()`][43]|Drop unused levels|[:material-notebook:][4]|
|[`levels()`][44]|Get levels of factors|[:material-notebook:][4]|
|[`is_factor()`][45] [`is_categorical`][45]|Test if data is factor|[:material-notebook:][4]|
|[`as_factor()`][46] [`as_categorical`][46]|Cast data to factor|[:material-notebook:][4]|
|[`is_ordered()`][140]|Check if a factor is ordered||
|[`nlevels()`][141]|Get number of levels of a factor||
|[`ordered()`][142]|Create an ordered factor||

### Logical/Boolean values

|API|Description|Notebook example|
|---|---|---:|
|`TRUE`|Logical true|[:material-notebook:][4]|
|`FALSE`|Logical false|[:material-notebook:][4]|
|[`is_true()`][47]|Test if data is scalar true (R's `isTRUE`)|[:material-notebook:][4]|
|[`is_false()`][48]|Test if data is scalar false (R's `FALSE`)|[:material-notebook:][4]|
|[`is_logical()`][49] [`is_bool()`][49]|Test if data is logical/boolean|[:material-notebook:][4]|
|[`as_logical()`][50] [`as_bool()`][50]|Cast data to logical/boolean|[:material-notebook:][4]|

### NA (missing values)

|API|Description|Notebook example|
|---|---|---:|
|`Inf`|Infinite number|[:material-notebook:][4]|
|`NA`|Missing value|[:material-notebook:][4]|
|`NaN`|Missing value, same as `NA`|[:material-notebook:][4]|
|[`is_na()`][51]|Test if data is NA|[:material-notebook:][4]|
|[`any_na()`][52]|Test if any element is NA|[:material-notebook:][4]|
|[`is_finite()`][126]|Test if x is finite||
|[`is_infinite()`][127]|Test if x is infinite||
|[`is_nan()`][128]|Test if x is nan||

### NULL

|API|Description|Notebook example|
|---|---|---:|
|`NULL`|NULL value|[:material-notebook:][4]|
|[`is_null()`][53]|Test if data is null|[:material-notebook:][4]|
|[`as_null()`][54]|Cast anything to NULL|[:material-notebook:][4]|

### Random

|API|Description|Notebook example|
|---|---|---:|
|[`set_seed()`][55]|Set the randomization seed|[:material-notebook:][4]|

### Functions to create and manipulate sequences

|API|Description|Notebook example|
|---|---|---:|
|[`c()`][56]|Collection of data|[:material-notebook:][4]|
|[`seq()`][57]|Generate sequence|[:material-notebook:][4]|
|[`seq_len()`][58]|Generate sequence with length|[:material-notebook:][4]|
|[`seq_along()`][59]|Generate sequence along with another sequence|[:material-notebook:][4]|
|[`rev()`][60]|Reverse a sequence|[:material-notebook:][4]|
|[`rep()`][61]|Generate sequence with repeats|[:material-notebook:][4]|
|[`lengths()`][62]|Get the length of elements in the sequence|[:material-notebook:][4]|
|[`unique()`][63]|Get the unique elements|[:material-notebook:][4]|
|[`sample()`][64]|Sample the elements from sequence|[:material-notebook:][4]|
|[`length()`][65]|Get the length of data|[:material-notebook:][4]|
|[`match()`][129]|match returns a vector of the positions of (first) matches of its first argument in its second.||
|[`rank()`][143]|Returns the sample ranks of the values in a vector.|[:material-notebook:][163]|
|[`order()`][144]|Returns a permutation which rearranges its first argument into ascending or descending order||
|[`sort()`][145]|Sorting or Ordering Vectors||

### Special functions

|API|Description|Notebook example|
|---|---|---:|
|[`beta()`][66]|Beta function|[:material-notebook:][4]|
|[`lbeta()`][67]|Natural logarithm of beta function|[:material-notebook:][4]|
|[`gamma()`][68]|Gamma function|[:material-notebook:][4]|
|[`lgamma()`][69]|Natural logarithm of gamma function|[:material-notebook:][4]|
|[`digamma()`][70]|the first derivatives of the logarithm of the gamma function.|[:material-notebook:][4]|
|[`trigamma()`][71]|the second derivatives of the logarithm of the gamma function.|[:material-notebook:][4]|
|[`psigamma()`][72]|polygamma funnction|[:material-notebook:][4]|
|[`choose()`][73]|binomial coefficients|[:material-notebook:][4]|
|[`lchoose()`][74]|the logarithms of binomial coefficients.|[:material-notebook:][4]|
|[`factorial()`][75]|factorial|[:material-notebook:][4]|
|[`lfactorial()`][76]|Natural logarithm of factorial|[:material-notebook:][4]|

### String functions

|API|Description|Notebook example|
|---|---|---:|
|[`is_character()`][77] [`is_str`][77] [`is_string`][77]|Test if data is string|[:material-notebook:][4]|
|[`as_character()`][78] [`as_str`][78] [`as_string`][78]|Cast data to string|[:material-notebook:][4]|
|[`grep()`][79]|Test if pattern in string|[:material-notebook:][4]|
|[`grepl()`][80]|Logical version of `grep`|[:material-notebook:][4]|
|[`sub()`][81]|Replace substrings in strings|[:material-notebook:][4]|
|[`gsub()`][82]|Replace all matched substring in strings|[:material-notebook:][4]|
|[`nchar()`][83]|Get length of string|[:material-notebook:][4]|
|[`nzhcar()`][84]|Test if string is not empty|[:material-notebook:][4]|
|[`paste()`][85]|Concatenate strings|[:material-notebook:][4]|
|[`paste0()`][86]|Concatenate strings with `sep=''`|[:material-notebook:][4]|
|[`sprintf()`][87]|C-style string formatting|[:material-notebook:][4]|
|[`substr()`][88]|Get substring|[:material-notebook:][4]|
|[`substring()`][89]|Get substring with a start only|[:material-notebook:][4]|
|[`strsplit()`][90]|Split strings with delimiter|[:material-notebook:][4]|
|[`startswith()`][130]|Test if strings start with given prefix||
|[`endswith()`][131]|Test if strings end with given suffix||
|[`strtoi()`][132]|Convert strings to integers||
|[`chartr()`][133]|Replace characters in strings||
|[`tolower()`][134]|Transform strings to lower case||
|[`toupper()`][135]|Transform strings to upper case||
|[`trimws()`][149]|Remove leading and/or trailing whitespace from character strings.||

### Table

|API|Description|Notebook example|
|---|---|---:|
|[`table()`][91]|Cross Tabulation and Table Creation|[:material-notebook:][4]|
|[`tabulate()`][146]|Takes the integer-valued vector `bin` and counts the number of times each integer occurs in it.||

### Testing value types

|API|Description|Notebook example|
|---|---|---:|
|[`is_double()`][92] [`is_float()`][92]|Test if data is double or float (`numpy.float_`)|[:material-notebook:][4]|
|[`is_integer()`][93] [`is_int()`][93]|Test if data is integer|[:material-notebook:][4]|
|[`is_numeric()`][94]|Test if data is numeric|[:material-notebook:][4]|
|[`is_atomic()`][95]|Test is data is atomic|[:material-notebook:][4]|
|[`is_element(), `is_in()`][96]|Test if value is an element of an array (R's `%in`)|[:material-notebook:][4]|

### Trigonometric and hyper bolic functions

|API|Description|Notebook example|
|---|---|---:|
|[`cos()`][97]|cosine|[:material-notebook:][4]|
|[`sin()`][98]|sine|[:material-notebook:][4]|
|[`tan()`][99]|tangent|[:material-notebook:][4]|
|[`acos()`][100]|Arc-cosine|[:material-notebook:][4]|
|[`asin()`][101]|Arc-sine|[:material-notebook:][4]|
|[`atan()`][102]|Arc-tangent|[:material-notebook:][4]|
|[`atan2()`][103]|`atan(y/x)`|[:material-notebook:][4]|
|[`cospi()`][104]|`cos(pi*x)`|[:material-notebook:][4]|
|[`sinpi()`][105]|`sin(pi*x)`|[:material-notebook:][4]|
|[`tanpi()`][106]|`tan(pi*x)`|[:material-notebook:][4]|
|[`cosh()`][107]|Hyperbolic cosine|[:material-notebook:][4]|
|[`sinh()`][108]|Hyperbolic sine|[:material-notebook:][4]|
|[`tanh()`][109]|Hyperbolic tangent|[:material-notebook:][4]|
|[`acosh()`][110]|Hyperbolic cosine|[:material-notebook:][4]|
|[`asinh()`][111]|Hyperbolic sine|[:material-notebook:][4]|
|[`atanh()`][112]|Hyperbolic tangent|[:material-notebook:][4]|

### Which

|API|Description|Notebook example|
|---|---|---:|
|[which()][1]|Which indices are True?|[:material-notebook:][4]|
|[which_min()][2] [which_max()][3]|Where is the minimum or maximum or first TRUE or FALSE|[:material-notebook:][4]|

### Other functions

|API|Description|Notebook example|
|---|---|---:|
|[`glimpse()`][166]|Get a glimpse of your data||
|[`cut()`][113]|Convert Numeric to Factor|[:material-notebook:][163]|
|[`diff()`][164]|Returns suitably lagged and iterated differences.|[:material-notebook:][163]|
|[`identity()`][114]|Identity Function|[:material-notebook:][163]|
|[`expandgrid()`][115]|Create a Data Frame from All Combinations of Factor Variables|[:material-notebook:][163]|
|[`outer()`][165]|Compute the outer product of two vectors.|[:material-notebook:][163]|
|[`max_col()`][136]|Find the maximum position for each row of a matrix||
|[`append()`][147]|Add elements to a vector.||
|[`complete_cases()`][137]|Get a bool array indicating whether the values of rows are complete in a data frame.||
|[`proportions()`][147], [`prop_table`][147]|Returns conditional proportions given `margins`||
|[`make_names()`][137]|Make names available as columns and can be accessed by `df.<name>`|[:material-notebook:][163]|
|[`make_unique()`][138]|Make the names unique, alias of `make_names(names, unique=True)`|[:material-notebook:][163]|


[1]: ../../api/datar.base.which/#datar.dplyr.which.which
[2]: ../../api/datar.base.which/#datar.dplyr.which.which_min
[3]: ../../api/datar.base.which/#datar.dplyr.which.which_max
[4]: ../../notebooks/base
[5]: ../../api/datar.core.options/#datar.core.options.options
[6]: ../../api/datar.core.options/#datar.core.options.get_option
[7]: ../../api/datar.core.options/#datar.core.options.options_context
[8]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.mean
[9]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.median
[10]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.min
[11]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.max
[12]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.pmin
[13]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.pmax
[14]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.sum
[15]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.abs
[16]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.round
[17]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.var
[18]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.ceiling
[19]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.floor
[20]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.sqrt
[21]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.cov
[22]: ../../api/datar.base.bessel/#datar.base.bessel.bessel_i
[23]: ../../api/datar.base.bessel/#datar.base.bessel.bessel_j
[24]: ../../api/datar.base.bessel/#datar.base.bessel.bessel_k
[25]: ../../api/datar.base.bessel/#datar.base.bessel.bessel_y
[26]: ../../api/datar.base.casting/#datar.base.casting.as_integer
[27]: ../../api/datar.base.casting/#datar.base.casting.as_double
[28]: ../../api/datar.base.casting/#datar.base.casting.as_float
[29]: ../../api/datar.base.casting/#datar.base.casting.as_numeric
[30]: ../../api/datar.base.complex/#datar.base.complex.re
[31]: ../../api/datar.base.complex/#datar.base.complex.mod
[32]: ../../api/datar.base.complex/#datar.base.complex.im
[33]: ../../api/datar.base.complex/#datar.base.complex.arg
[34]: ../../api/datar.base.complex/#datar.base.complex.conj
[35]: ../../api/datar.base.complex/#datar.base.complex.is_complex
[36]: ../../api/datar.base.complex/#datar.base.complex.as_complex
[37]: ../../api/datar.base.cum/#datar.base.cum.cumsum
[38]: ../../api/datar.base.cum/#datar.base.cum.cumprod
[39]: ../../api/datar.base.cum/#datar.base.cum.cummin
[40]: ../../api/datar.base.cum/#datar.base.cum.cummax
[41]: ../../api/datar.base.date/#datar.base.date.as_date
[42]: ../../api/datar.base.factor/#datar.base.factor.factor
[43]: ../../api/datar.base.factor/#datar.base.factor.droplevels
[44]: ../../api/datar.base.factor/#datar.base.factor.levels
[45]: ../../api/datar.base.factor/#datar.base.factor.is_factor
[46]: ../../api/datar.base.factor/#datar.base.factor.as_factor
[47]: ../../api/datar.base.logical/#datar.base.logical.is_true
[48]: ../../api/datar.base.logical/#datar.base.logical.is_false
[49]: ../../api/datar.base.logical/#datar.base.logical.is_logical
[50]: ../../api/datar.base.logical/#datar.base.logical.as_logical
[51]: ../../api/datar.base.na/#datar.base.na.is_na
[52]: ../../api/datar.base.na/#datar.base.na.any_na
[53]: ../../api/datar.base.null/#datar.base.null.is_null
[54]: ../../api/datar.base.null/#datar.base.null.as_null
[55]: ../../api/datar.base.random/#datar.base.random.set_seed
[56]: ../../api/datar.base.seq/#datar.base.seq.c
[57]: ../../api/datar.base.seq/#datar.base.seq.seq
[58]: ../../api/datar.base.seq/#datar.base.seq.seq_len
[59]: ../../api/datar.base.seq/#datar.base.seq.seq_along
[60]: ../../api/datar.base.seq/#datar.base.seq.rev
[61]: ../../api/datar.base.seq/#datar.base.seq.rep
[62]: ../../api/datar.base.seq/#datar.base.seq.lengths
[63]: ../../api/datar.base.seq/#datar.base.seq.unique
[64]: ../../api/datar.base.seq/#datar.base.seq.sample
[65]: ../../api/datar.base.seq/#datar.base.seq.length
[66]: ../../api/datar.base.special/#datar.base.special.beta
[67]: ../../api/datar.base.special/#datar.base.special.lbeta
[68]: ../../api/datar.base.special/#datar.base.special.gamma
[69]: ../../api/datar.base.special/#datar.base.special.lgamma
[70]: ../../api/datar.base.special/#datar.base.special.digamma
[71]: ../../api/datar.base.special/#datar.base.special.trigamma
[72]: ../../api/datar.base.special/#datar.base.special.psigamma
[73]: ../../api/datar.base.special/#datar.base.special.choose
[74]: ../../api/datar.base.special/#datar.base.special.lchoose
[75]: ../../api/datar.base.special/#datar.base.special.factorial
[76]: ../../api/datar.base.special/#datar.base.special.lfactorial
[77]: ../../api/datar.base.string/#datar.base.string.is_character
[78]: ../../api/datar.base.string/#datar.base.string.as_character
[79]: ../../api/datar.base.string/#datar.base.string.grep
[80]: ../../api/datar.base.string/#datar.base.string.grepl
[81]: ../../api/datar.base.string/#datar.base.string.sub
[82]: ../../api/datar.base.string/#datar.base.string.gsub
[83]: ../../api/datar.base.string/#datar.base.string.nchar
[84]: ../../api/datar.base.string/#datar.base.string.nzchar
[85]: ../../api/datar.base.string/#datar.base.string.paste
[86]: ../../api/datar.base.string/#datar.base.string.paste0
[87]: ../../api/datar.base.string/#datar.base.string.sprintf
[88]: ../../api/datar.base.string/#datar.base.string.substr
[89]: ../../api/datar.base.string/#datar.base.string.substring
[90]: ../../api/datar.base.string/#datar.base.string.strsplit
[91]: ../../api/datar.base.table/#datar.base.table.table
[92]: ../../api/datar.base.testing/#datar.base.testing.is_double
[93]: ../../api/datar.base.testing/#datar.base.testing.is_float
[94]: ../../api/datar.base.testing/#datar.base.testing.is_integer
[95]: ../../api/datar.base.testing/#datar.base.testing.is_atomic
[96]: ../../api/datar.base.testing/#datar.base.testing.is_element
[97]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.cos
[98]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.sin
[99]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.tan
[100]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.acos
[101]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.asin
[102]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.atan
[103]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.atan2
[104]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.cospi
[105]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.sinpi
[106]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.tanpi
[107]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.cosh
[108]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.sinh
[109]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.tanh
[110]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.acosh
[111]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.asinh
[112]: ../../api/datar.base.trig_hb/#datar.base.trig_hb.atanh
[113]: ../../api/datar.base.funs/#datar.base.funs.cut
[114]: ../../api/datar.base.funs/#datar.base.funs.identity
[115]: ../../api/datar.base.funs/#datar.base.funs.expandgrid
[116]: ../../api/datar.base.funs/#datar.base.funs.data_context
[117]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.prod
[118]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.sign
[119]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.trunc
[120]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.exp
[121]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.log
[122]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.log2
[123]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.log10
[124]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.log1p
[125]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.signif
[126]: ../../api/datar.base.na/#datar.base.na.is_finite
[127]: ../../api/datar.base.na/#datar.base.na.is_infinite
[128]: ../../api/datar.base.na/#datar.base.na.is_nan
[129]: ../../api/datar.base.seq/#datar.base.seq.match
[130]: ../../api/datar.base.string/#datar.base.string.startswith
[131]: ../../api/datar.base.string/#datar.base.string.endswith
[132]: ../../api/datar.base.string/#datar.base.string.strtoi
[133]: ../../api/datar.base.string/#datar.base.string.chartr
[134]: ../../api/datar.base.string/#datar.base.string.tolower
[135]: ../../api/datar.base.string/#datar.base.string.toupper
[136]: ../../api/datar.base.verbs/#datar.base.verbs.max_col
[137]: ../../api/datar.base.verbs/#datar.base.verbs.complete_cases
[138]: ../../api/datar.base.funs/#datar.base.funs.make_names
[139]: ../../api/datar.base.funs/#datar.base.funs.make_unique
[140]: ../../api/datar.base.factor/#datar.base.factor.is_ordered
[141]: ../../api/datar.base.factor/#datar.base.factor.nlevels
[142]: ../../api/datar.base.factor/#datar.base.factor.ordered
[143]: ../../api/datar.base.funs/#datar.base.funs.rank
[144]: ../../api/datar.base.seq/#datar.base.seq.order
[145]: ../../api/datar.base.seq/#datar.base.seq.sort
[146]: ../../api/datar.base.table/#datar.base.table.tabulate
[147]: ../../api/datar.base.verbs/#datar.base.verbs.append
[148]: ../../api/datar.base.verbs/#datar.base.verbs.proportions
[149]: ../../api/datar.base.string/#datar.base.string.trimws
[150]: ../../api/datar.base.date/#datar.base.date.as_pd_date
[151]: ../../notebooks/base-arithmetic
[152]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.quantile
[153]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.sd
[154]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.weighted_mean
[155]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.col_sums
[156]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.row_sums
[157]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.col_means
[158]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.row_means
[159]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.col_sds
[160]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.row_sds
[161]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.col_medians
[162]: ../../api/datar.base.arithmetic/#datar.base.arithmetic.row_medians
[163]: ../../notebooks/base-funs
[164]: ../../api/datar.base.funs/#datar.base.funs.diff
[165]: ../../api/datar.base.funs/#datar.base.funs.outer
[166]: ../../api/datar.base.glimpse/#datar.base.glimpse.glimpse
