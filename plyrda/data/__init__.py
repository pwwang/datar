from pathlib import Path

import pandas

HERE = Path(__file__).parent
WITH_INDEX = ['mtcars']

def all_datasets():
    return [datafile.name[:-7] for datafile in HERE.glob('*.csv.gz')]

def load_data(name):
    datafile = HERE / f'{name}.csv.gz'
    if not datafile.is_file():
        raise ImportError(f'No such dataset: {name}, '
                          f'available are: {all_datasets()}')
    return pandas.read_csv(datafile,
                           index_col=0 if name in WITH_INDEX else False)

__all__ = all_datasets()

from modkit import modkit
@modkit.delegate
def delegate(module, name):
    # for mkapi to work
    if name.startswith('_'): # pragma: no cover
        raise AttributeError
    return load_data(name)
