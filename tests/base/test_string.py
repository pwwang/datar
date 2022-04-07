import pytest

import numpy as np
from datar.base.string import (
    as_character,
    is_character,
    grep,
    grepl,
    gsub,
    chartr,
    endswith,
    nchar,
    nzchar,
    paste,
    paste0,
    sprintf,
    startswith,
    strsplit,
    strtoi,
    sub,
    substr,
    substring,
    tolower,
    toupper,
    trimws,
)
from datar.base.constants import letters, LETTERS
from datar.tibble import tibble
from ..conftest import assert_iterable_equal


def test_as_character():
    assert as_character(1) == "1"
    assert_iterable_equal(as_character([1, 2]), ["1", "2"])
    assert_iterable_equal(as_character([1, np.nan]), ["1", np.nan])


def test_is_character():
    assert not is_character(1)
    assert not is_character([1, "a"])
    assert is_character("111")
    assert is_character(["111", "222"])


def test_grep():
    out = grep("[a-z]", "a")
    assert_iterable_equal(out, [0])
    out = grep("[a-z]", "a", invert=True)
    assert_iterable_equal(out, [])

    out = grep("[a-z]", letters)
    assert_iterable_equal(out, list(range(26)))

    txt = ["arm", "foot", "lefroo", "bafoobar"]
    out = grep("foo", txt)
    assert_iterable_equal(out, [1, 3])

    out = grep("foo", txt, value=True)
    assert_iterable_equal(out, ["foot", "bafoobar"])

    # fixed
    out = grep(".", "a")
    assert_iterable_equal(out, [0])
    out = grep(".", "a", fixed=True)
    assert_iterable_equal(out, [])


def test_grepl(caplog):
    txt = ["arm", "foot", "lefroo", "bafoobar"]
    out = grepl(["foo"], txt)
    assert_iterable_equal(out, [False, True, False, True])

    assert_iterable_equal(grepl("a", "a"), [True])

    # warn # patterns > 1
    grepl(["a", "b"], "a")
    assert "has length > 1" in caplog.text

    # np.nan
    out = grepl("a", ["a", np.nan])
    assert_iterable_equal(out, [True, False])


def test_sub():
    txt = ["arm", "foot", "lefroo", "bafoobarfoo"]
    out = sub("foo", "bar", txt)
    assert_iterable_equal(out, ["arm", "bart", "lefroo", "babarbarfoo"])

    # fixed
    out = sub(".", "a", "b")
    assert_iterable_equal(out, ["a"])
    out = sub(".", "a", "b", fixed=True)
    assert_iterable_equal(out, "b")


def test_gsub():
    txt = ["arm", "foot", "lefroo", "bafoobarfoo"]
    out = gsub("foo", "bar", txt)
    assert_iterable_equal(out, ["arm", "bart", "lefroo", "babarbarbar"])


def test_nchar():
    s = "\u200b"
    assert_iterable_equal(nchar(s), [1])
    assert_iterable_equal(nchar(s, type="bytes"), [3])
    assert_iterable_equal(nchar(s, type="width"), [0])

    with pytest.raises((ValueError, UnicodeEncodeError)):
        nchar("\ud861", type="bytes", allow_na=False)

    out = nchar(["\ud861", np.nan, "a"], type="bytes")
    assert_iterable_equal(out, [np.nan, np.nan, 1])


def test_nzchar():
    assert_iterable_equal(nzchar(""), [False])
    assert_iterable_equal(nzchar("1"), [True])
    assert_iterable_equal(nzchar(["1", np.nan]), [True, False])
    assert_iterable_equal(nzchar(["1", np.nan], keep_na=True), [True, np.nan])


def test_paste():
    assert_iterable_equal(paste("a", "b"), ["a b"])
    assert_iterable_equal(paste("a", ["b", "c"]), ["a b", "a c"])
    assert paste("a", ["b", "c"], collapse="_") == "a b_a c"
    assert paste0("a", ["b", "c"], collapse="_") == "ab_ac"
    with pytest.raises(ValueError):
        paste(list("ab"), list("abc"))

    df = tibble(a=letters[:3], b=LETTERS[:3]).rowwise()
    out = paste0(df)
    assert_iterable_equal(out, ["aA", "bB", "cC"])

    out = paste([], [])
    assert_iterable_equal(out, [])

    out = paste0([], ["a"])
    assert_iterable_equal(out, ["a"])

    df = tibble(x=[1, 2, 3], y=[4, 5, 5])
    out = paste(df)
    assert_iterable_equal(out, ["1 4", "2 5", "3 5"])

    gf = df.group_by("y")
    out = paste0(gf, collapse="|")
    assert_iterable_equal(out, ['14', '25|35'])


def test_sprintf():
    assert_iterable_equal(sprintf("%d", 1.1), ["1"])
    assert_iterable_equal(sprintf(["%d", "%.2f"], 1.1), ["1", "1.10"])
    assert_iterable_equal(sprintf(["%d", "%.2f"], 2.345), ["2", "2.35"])
    assert_iterable_equal([sprintf(np.nan, 1.1)], [np.nan])
    df = tibble(x=["%d", "%.2f"], y=[1.1, 2.345]).group_by("x")
    assert_iterable_equal(sprintf(df.x, df.y).obj, ["1", "2.35"])


def test_substr():
    assert_iterable_equal(substr("abcd", 1, 3), ["bc"])
    assert_iterable_equal(substr(np.nan, 1, 3), [np.nan])
    # assert_iterable_equal(substr(np.nan, [1,2], 3), [np.nan, np.nan])
    assert_iterable_equal(substr(["abce", "efgh"], 1, 3), ["bc", "fg"])
    assert_iterable_equal(
        substr(["abce", "efgh", np.nan], 1, 3), ["bc", "fg", np.nan]
    )
    assert_iterable_equal(substring("abcd", 1), ["bcd"])
    assert_iterable_equal(substring(np.nan, 1), [np.nan])


def test_strsplit():
    assert_iterable_equal(
        strsplit("a.b.c", ".", fixed=True)[0],
        ["a", "b", "c"]
    )
    out = strsplit("a.b.c", ".", fixed=False)
    assert_iterable_equal(
        out[0],
        [""] * 6
    )



def test_starts_endswith():
    assert_iterable_equal(startswith("abc", "a"), [True])
    assert_iterable_equal(endswith("abc", "c"), [True])
    assert_iterable_equal(startswith(["abc", "def"], "a"), [True, False])
    assert_iterable_equal(endswith(["abc", "def"], "c"), [True, False])


def test_strtoi():
    assert_iterable_equal(strtoi("8"), [8])
    assert_iterable_equal(strtoi("0b111"), [7])
    assert_iterable_equal(strtoi("0xf"), [15])
    assert_iterable_equal(strtoi(["8", "0b111", "0xf"]), [8, 7, 15])


def test_chartr(caplog):
    assert_iterable_equal(chartr("a", "A", "abc"), ["Abc"])
    chartr(["a", "b"], ["A", "B"], "abc")
    assert "In chartr" in caplog.text
    caplog.clear()

    chartr(["a", "b"], "A", "abc")
    assert "In chartr" in caplog.text

    assert_iterable_equal(chartr("a", "A", ["abc", "ade"]), ["Abc", "Ade"])

    with pytest.raises(ValueError):
        chartr("abc", "x", "z")


def test_transform_case():
    assert_iterable_equal(tolower("aBc"), ["abc"])
    assert_iterable_equal(toupper("aBc"), ["ABC"])
    assert_iterable_equal(tolower(["aBc", "DeF"]), ["abc", "def"])
    assert_iterable_equal(toupper(["aBc", "DeF"]), ["ABC", "DEF"])


def test_trimws():
    assert_iterable_equal(trimws(" a "), ["a"])
    assert_iterable_equal(trimws([" a ", " b "], "both"), ["a", "b"])
    assert_iterable_equal(trimws([" a ", " b "], "left"), ["a ", "b "])
    assert_iterable_equal(trimws([" a ", " b "], "right"), [" a", " b"])
