import pytest

from datar.base.string import *
from datar.base.constants import letters
from .conftest import assert_iterable_equal

def test_as_character():
    assert as_character(1) == '1'
    assert_iterable_equal(as_character([1,2]), ['1', '2'])
    assert_iterable_equal(as_character([1,NA]), ['1', NA])

def test_is_character():
    assert not is_character(1)
    assert not is_character([1, 'a'])
    assert is_character('111')
    assert is_character(['111', '222'])

def test_grep():
    out = grep('[a-z]', 'a')
    assert_iterable_equal(out, [0])
    out = grep('[a-z]', 'a', base0_=False)
    assert_iterable_equal(out, [1])
    out = grep('[a-z]', 'a', invert=True)
    assert_iterable_equal(out, [])

    out = grep('[a-z]', letters)
    assert_iterable_equal(out, list(range(26)))

    txt = ["arm","foot","lefroo", "bafoobar"]
    out = grep("foo", txt)
    assert_iterable_equal(out, [1,3])

    out = grep("foo", txt, value=True)
    assert_iterable_equal(out, ["foot", "bafoobar"])

    # fixed
    out = grep(".", "a")
    assert_iterable_equal(out, [0])
    out = grep(".", "a", fixed=True)
    assert_iterable_equal(out, [])

def test_grepl(caplog):
    txt = ["arm","foot","lefroo", "bafoobar"]
    out = grepl(["foo"], txt)
    assert_iterable_equal(out, [False, True, False, True])

    assert_iterable_equal(grepl('a', 'a'), [True])

    # warn # patterns > 1
    grepl(['a', 'b'], 'a')
    assert 'has length > 1' in caplog.text

    # NA
    out = grepl('a', ['a', NA])
    assert_iterable_equal(out, [True, False])


def test_sub():
    txt = ["arm","foot","lefroo", "bafoobarfoo"]
    out = sub("foo", "bar", txt)
    assert_iterable_equal(out, ["arm", "bart", "lefroo", "babarbarfoo"])

    # fixed
    out = sub(".", "a", "b")
    assert_iterable_equal(out, ["a"])
    out = sub(".", "a", "b", fixed=True)
    assert_iterable_equal(out, ["b"])

def test_gsub():
    txt = ["arm","foot","lefroo", "bafoobarfoo"]
    out = gsub("foo", "bar", txt)
    assert_iterable_equal(out, ["arm", "bart", "lefroo", "babarbarbar"])

def test_nchar():
    s = '\u200b'
    assert_iterable_equal(nchar(s), [1])
    assert_iterable_equal(nchar(s, type="bytes"), [3])
    assert_iterable_equal(nchar(s, type="width"), [0])

    with pytest.raises(UnicodeEncodeError):
        nchar('\ud861', type="bytes", allow_na=False)

    out = nchar(['\ud861', NA, 'a'], type="bytes")
    assert_iterable_equal(out, [NA, NA, 1])
    assert out.dtype == object

def test_nzchar():
    assert_iterable_equal(nzchar(''), [False])
    assert_iterable_equal(nzchar('1'), [True])
    assert_iterable_equal(nzchar(['1', NA]), [True, True])
    assert_iterable_equal(nzchar(['1', NA], keep_na=True), [True, NA])

def test_paste():
    assert_iterable_equal(paste("a", "b"), ["a b"])
    assert_iterable_equal(paste("a", ["b", "c"]), ["a b", "a c"])
    assert paste("a", ["b", "c"], collapse="_") == "a b_a c"
    assert paste0("a", ["b", "c"], collapse="_") == "ab_ac"


def test_sprintf():
    assert sprintf("%d", 1.1) == '1'
    assert_iterable_equal(sprintf(["%d", "%.2f"], [1.1, 2.345]), ["1", "2.35"])

def test_substr():
    assert substr('abcd', 2, 3) == 'bc'
    assert_iterable_equal([substr(NA, 2, 3)], [NA])
    assert_iterable_equal(substr(NA, [2,3], 3), [NA, NA])
    assert_iterable_equal(substr(['abce', 'efgh'], 2, 3), ['bc', 'fg'])
    assert_iterable_equal(substr(['abce', 'efgh', NA], 2, 3), ['bc', 'fg', NA])
    assert substring('abcd', 2) == 'bcd'

def test_strsplit():
    assert strsplit('a.b.c', '.', fixed=True) == ['a', 'b', 'c']
    out = strsplit('a.b.c', ['.', 'b'], fixed=True)
    assert len(out) == 2
    assert out[0] == ['a', 'b', 'c']
    assert out[1] == ['a.', '.c']

def test_starts_endswith():
    assert startswith("abc", "a")
    assert endswith("abc", "c")
    assert_iterable_equal(startswith(["abc", "def"], "a"), [True, False])
    assert_iterable_equal(endswith(["abc", "def"], "c"), [True, False])

def test_strtoi():
    assert strtoi("8") == 8
    assert strtoi("0b111") == 7
    assert strtoi("0xf") == 15
    assert_iterable_equal(strtoi(["8", "0b111", "0xf"]), [8, 7, 15])

def test_chartr():
    assert chartr("a", "A", "abc") == "Abc"
    with pytest.warns(UserWarning):
        chartr(["a", "b"], ["A", "B"], "abc")
    with pytest.raises(ValueError):
        chartr(["a", "b"], "A", "abc")

    assert_iterable_equal(
        chartr("a", "A", ["abc", "ade"]),
        ["Abc", "Ade"]
    )

def test_transform_case():
    assert tolower("aBc") == "abc"
    assert toupper("aBc") == "ABC"
    assert_iterable_equal(tolower(["aBc", "DeF"]), ["abc", "def"])
    assert_iterable_equal(toupper(["aBc", "DeF"]), ["ABC", "DEF"])

def test_trimws():
    assert trimws(" a ") == "a"
    assert_iterable_equal(trimws([" a ", " b "], "both"), ["a", "b"])
    assert_iterable_equal(trimws([" a ", " b "], "left"), ["a ", "b "])
    assert_iterable_equal(trimws([" a ", " b "], "right"), [" a", " b"])
