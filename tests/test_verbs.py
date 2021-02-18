from plyrda import f
from plyrda.data import diamonds, starwars
from plyrda.verbs import *
from plyrda.funcs import starts_with
from plyrda.commons import c

def test_select():
    # 'carat', 'cut', 'color', 'clarity', 'depth', 'table', 'price', 'x', 'y', 'z'
    x = diamonds >> select(1, f.price, ['x', 'y']) >> head(2)
    assert x.columns.to_list() == ['cut', 'price', 'x', 'y']
    assert x.shape == (2, 4)

    x = diamonds >> select(c(f.cut, f.price)) >> head(2)
    assert x.columns.to_list() == ['cut', 'price']

    x = diamonds >> select(-c(f.cut, f.price)) >> head(2)
    assert 'cut' not in x.columns.to_list()
    assert 'price' not in x.columns.to_list()
    x = diamonds >> select(-f.price) >> head(2)
    assert 'price' not in x.columns.to_list()

    x = diamonds >> select(starts_with('c')) >> head(2)
    assert x.columns.to_list() == ['carat','cut', 'color', 'clarity']

    x = diamonds >> select(-starts_with('c')) >> head(2)
    assert x.columns.to_list() == ['depth', 'table', 'price', 'x', 'y', 'z']

    x = diamonds >> select(None//f.cut, 'depth', f.y//None) >> head(2)
    assert x.columns.to_list() == ['carat', 'cut', 'depth', 'y', 'z']

    x = diamonds >> select(f.cut, a=f.carat)
    assert x.columns.to_list() == ['cut', 'a']

    # name               height  mass hair_color    skin_color  eye_color
    # birth_year sex    gender    homeworld species films     vehicles  starships
    x = starwars >> select(f.name // f.mass)
    assert x.columns.to_list() == ['name', 'height', 'mass']
