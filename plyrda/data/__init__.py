from pathlib import Path

import pandas
from modkit import modkit

HERE = Path(__file__).parent

def all_datasets():
    return [datafile.name[:-7] for datafile in HERE.glob('*.csv.gz')]

def load_data(name):
    datafile = HERE / f'{name}.csv.gz'
    if not datafile.is_file():
        raise ImportError(f'No such dataset: {name}, '
                          f'available are: {all_datasets()}')
    return pandas.read_csv(datafile)

__all__ = all_datasets()

@modkit.delegate
def delegate(module, name):
    # for mkapi to work
    if name in ('__wrapped__', '__qualname__', '__signature__'):
        raise AttributeError
    return load_data(name)
