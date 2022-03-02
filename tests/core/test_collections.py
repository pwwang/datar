import pytest

from datar.core.collections import Collection, Intersect, Inverted, Slice


def test_collections_flattened():
    cs = Collection(Collection(3, 4))
    assert cs == [3, 4]
    cs = Collection(1, 2, Collection(3, 4))
    assert cs == [1, 2, 3, 4]
    cs = Collection(1, 2, Collection(3, Collection(4)))
    assert cs == [1, 2, 3, 4]
    cs = Collection(1, 2, [3, Collection(4)])
    assert cs == [1, 2, 3, 4]
    # empty
    cs = Collection()
    assert cs == []


def test_collections_expand_to_indexes_with_pool():
    cs = Collection(1, 2, Collection(3, 4))
    cs = Collection(cs, pool=5)
    assert cs == [1, 2, 3, 4]
    cs = Collection(cs, pool=[4, 3, 2, 1])
    assert cs == [3, 2, 1, 0]

    cs = Collection(-Collection(1))
    assert list(cs) == [-1]
    cs.expand(pool=10)
    assert list(cs) == [9]

    cs = Collection(-Collection(1), pool=10)
    assert list(cs) == [9]


def test_collection_of_slice_expands_correctly():
    cs = Collection(1, slice(3, 5))
    assert cs == [1, 3, 4]


def test_unmatched_saved():
    cs = Collection(5, pool=4)
    assert cs.unmatched == {5}


def test_strings_turn_into_integers_with_string_pool():
    cs = Collection(list("bcd"), pool=list("abc"))
    assert cs == [1, 2]
    assert cs.unmatched == {"d"}
    assert "Collection" in repr(cs)
    assert "[1, 2]" == str(cs)


# Negated ----------------------------------------------------------------
def test_negate_collection():
    # cs = -Collection(1, 2)
    # assert cs == [-1, -2]

    cs = -Collection(0, 1, pool=2)
    # pool: 0, 1  with 1, 2, we get 1
    #      -2, -1
    #       0, 1
    assert list(cs) == [0, 1]

    cs = -Collection(1, 2, pool=[])
    assert cs == []
    assert cs.unmatched == {1, 2}

    assert "Negated" in repr(cs)

    cs = -Collection(list("abc"), pool=list("bcd"))
    # 'abc' mapping to 'bcd' => [0,1]
    # Negated([0,1], pool=3) => [-2,-1] => [1,2]
    assert list(cs) == [1, 2]
    assert cs.unmatched == {"a"}

    # cannot negate strings
    with pytest.raises(TypeError):
        -Collection("a")

    cs = -Collection("c", pool=["a", "b"])
    assert cs == []
    assert cs.unmatched == {"c"}


def test_mixed_negatives_and_positives():
    cs = Collection(-Collection(1, 3), 4)
    assert cs == [-1, -3, 4]

    # 0   1   2   3   4
    #        -3      -1
    #             3
    cs = Collection(-Collection(1, 3), 3, pool=5)
    assert list(cs) == [4, 2, 3]


# Inverted ----------------------------------------------------------------


def test_invert_collection():
    cs = ~Collection(1, 2)
    # cannot expand without pool
    assert cs == []
    assert "Inverted" in repr(cs)

    # non-index gets excluded literally, but returns indexes
    cs = ~Collection("a", "b", pool=list("abc"))
    assert cs == [2]

    cs = Inverted(0, 1, 2, 3, pool=5)
    assert cs == [4]


def test_inverted_collections_elements_get_unioned_then_inverted():
    cs = Collection(~Collection(1, 2), ~Collection(3, 4), pool=5)
    assert list(cs) == [0]

    with pytest.raises(ValueError):
        Collection(1, ~Collection(2)).expand(pool=3)


# Slice ----------------------------------------------------------------
class S:
    def __getitem__(self, s):
        return s


@pytest.fixture
def s():
    return S()


def test_slice_cannot_wrap_non_slice_obj():
    with pytest.raises(ValueError):
        Slice(1)


def test_slice_expand_literally_without_pool(s):
    slc = Slice(s[1:3])
    assert slc == [1, 2]
    assert "Slice" in repr(slc)

    slc = Slice(s[:3])
    assert slc == [0, 1, 2]

    slc = Slice(s[3:])
    assert slc == [3, 2, 1]


def test_slice_pool_required_for_str_start_stop(s):
    slc = Slice(s["a":"c"])
    assert slc == []

    with pytest.raises(ValueError):
        slc.expand()


def test_slice_expands_with_pool(s):
    slc = Slice(s["b":"d"], pool=list("abcd"))
    assert list(slc) == [1, 2]
    slc = Slice(s["b":], pool=list("abcd"))
    assert list(slc) == [1, 2, 3]
    slc = Slice(s[:"c"], pool=list("abcd"))
    assert list(slc) == [0, 1]
    slc = Slice(s[:], pool=list("abcd"))
    assert list(slc) == [0, 1, 2, 3]
    slc = Slice(s[:"e"], pool=list("abcd"))
    assert slc == []
    with pytest.raises(KeyError):
        slc.expand(pool=list("abcd"))
    slc = Slice(s["e":], pool=list("abcd"))
    assert slc == []
    with pytest.raises(KeyError):
        slc.expand(pool=list("abcd"))


def test_slice_with_int_pool(s):
    slc = Collection(s[1:3], pool=4)
    assert list(slc) == [1, 2]
    slc = Collection(s[:3], pool=4)
    assert list(slc) == [0, 1, 2]
    slc = Collection(s[1:], pool=4)
    assert list(slc) == [1, 2, 3]
    slc = Collection(s[:], pool=4)
    assert list(slc) == [0, 1, 2, 3]


def test_negated_slice(s):
    cs = Inverted(s[:2], pool=3)
    # s[:2] => 0,1 ==index==> 0,1
    # pool=3 => [0,1,2]
    # so cs == [2]
    assert list(cs) == [2]


def test_mixed_collections(s):
    cs = Collection(1, 2, s[3:5], -Collection(1, 2))
    assert cs == [1, 2, 3, 4, -1, -2]

    cs = Collection(
        3, Collection(5), s[6:7], -Collection(1), pool=10
    )
    assert list(cs) == [3, 5, 6, 9]


# Intersect -------------------------------
def test_intersect():
    # Intersect has to have 2 arguments
    with pytest.raises(ValueError):
        Intersect(1)

    # without pool
    left = Collection(1, 2, 3)
    right = Collection(2, 3, 4)
    ins = Intersect(left, right).expand()
    assert list(ins) == [2, 3]
    assert "Intersect" in repr(ins)

    # indexes work
    left = Collection(1, 2, 3, pool=list("abcd"))
    right = Collection(2, 3, 4, pool=list("abcd"))
    ins = Intersect(left, right).expand()
    assert list(ins) == [2, 3]  # returns 0-based indexes

    # mixed str and index get correctly intersected
    left = Collection(0, 1, "c", pool=list("abcd"))
    right = Collection("b", 2, 3, pool=list("abcd"))
    ins = Intersect(left, right).expand()
    assert list(ins) == [1, 2]
