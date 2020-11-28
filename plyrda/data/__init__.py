from pathlib import Path

import pandas
from modkit import modkit

HERE = Path(__file__).parent

def all_datasets():
    return [datafile.stem for datafile in HERE.glob('*.csv')]

def load_data(name):
    datafile = HERE / f'{name}.csv'
    if not datafile.is_file():
        raise ImportError(f'No such dataset: {name}, '
                          f'available are: {all_datasets()}')
    return pandas.read_csv(datafile)

__all__ = all_datasets()

@modkit.delegate
def delegate(module, name):
    return load_data(name)
