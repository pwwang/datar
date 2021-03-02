from pathlib import Path

import pandas
from modkit import install

HERE = Path(__file__).parent
WITH_INDEX = ['mtcars']

def all_datasets():
    return [datafile.name[:-7] for datafile in HERE.glob('*.csv.gz')]

def load_data(name):
    datafile = HERE / f'{name}.csv.gz'
    if not datafile.is_file():
        raise ImportError(f'No such dataset: {name}, '
                          f'available are: {all_datasets()}')
    data = pandas.read_csv(datafile,
                           index_col=0 if name in WITH_INDEX else False)
    data.__dfname__ = name
    return data

__all__ = all_datasets()

def __getattr__(name):
    return load_data(name)

install(__name__)
