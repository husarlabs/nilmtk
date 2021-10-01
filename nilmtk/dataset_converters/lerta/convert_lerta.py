'''
Lerta Electrical Energy Dataset


Lerta dataset converter for the clean version avaiable at the URLs below:
https://github.com/husarlabs/nilm-dataset-converter

The original version of the dataset include outliers, different sampling and missing values.

Check the dataset website for more information.
........url.....


'''

import datetime
from sys import stdout

import pandas as pd
import numpy as np
from os.path import join, exists, isdir
from os import  listdir
import yaml
from typing import List

from nilmtk.utils import get_datastore
from nilmtk.datastore import Key
from nilmtk.measurement import LEVEL_NAMES
from nilmtk.utils import get_module_directory, check_directory_exists
from nilm_metadata import save_yaml_to_datastore
from nilmtk.datastore import DataStore

def convert_lerta(input_path: str, output_filename: str, format: str ='HDF') -> None:
    """
    Parameters
    ----------
    input_path : str
        The root path of the CSV files, e.g. House1.csv
    output_filename : str
        The destination filename (including path and suffix).
    format : str
        format of output. Either 'HDF' or 'CSV'. Defaults to 'HDF'
    """

    # Open DataStore
    store = get_datastore(output_filename, format, mode='w')
    type(store)
    # Convert raw data to DataStore
    _convert(input_path, store, 'Europe/Warsaw')

    # Add metadata
    save_yaml_to_datastore(join(get_module_directory(),
                                'dataset_converters',
                                'lerta',
                                'metadata'),
                           store)
    store.close()

    print('Done converting Lerta to HDF5!')

def _find_all_houses(input_path: str) -> List[str]:
    """
    Returns
    -------
    list of string (houses)
    """
    houses = list(set([p.split('_')[0] for p in listdir(input_path) if p.startswith('House')]))

    return sorted(houses)

def _convert(input_path: str, store: DataStore, tz: str, sort_index: bool=False) -> None:
    """
    Parameters
    ----------
    input_path : str
        The root path of the REFIT dataset.
    store : DataStore
        The NILMTK DataStore object.
    measurement_mapping_func : function
        Must take these parameters:
            - house_id
            - chan_id
        Function should return a list of tuples e.g. [('power', 'active')]
    tz : str
        Timezone e.g. 'US/Eastern'
    sort_index : bool
    """

    check_directory_exists(input_path)
    for nilmtk_house_id, house_name in enumerate(_find_all_houses(input_path), 1):
        print(f'Loading house: {house_name}')

        house_metadata_path = join(get_module_directory(), 'dataset_converters', 'lerta',
                                   f'metadata/building{house_name[-1]}.yaml')
        if not exists(house_metadata_path):
            raise RuntimeError(f'Could not find {house_name} metadata.')
        with open(house_metadata_path, 'r') as stream:
            house_metadata = yaml.safe_load(stream)

        csv_filename = join(input_path, f'CLEAN_{house_name}.csv')
        if not exists(csv_filename):
            print(f'Can not find CLEAN_{house_name}. '
                  f'Convert raw data using: https://github.com/husarlabs/nilm-dataset-converter')
            continue
        usecols = ['AGGREGATE']
        appliance_columns = list(set([name.get('original_name') for name in house_metadata.get('appliances')]))
        usecols.extend(appliance_columns)
        df = _load_csv(csv_filename, usecols, tz)
        if sort_index:
            df = df.sort_index()
        for chan_id, col in enumerate(df.columns, 1):
            print(chan_id, end=" ")
            stdout.flush()
            key = Key(building=nilmtk_house_id, meter=chan_id)

            chan_df = pd.DataFrame(df[col])
            chan_df.columns = pd.MultiIndex.from_tuples([('power', 'active')])

            # Modify the column labels to reflect the power measurements recorded.
            chan_df.columns.set_names(LEVEL_NAMES, inplace=True)
            store.put(str(key), chan_df)

        print('')


def _load_csv(filename: str, usecols: List, tz: str) -> pd.DataFrame:
    """
    Parameters
    ----------
    filename : str
    usecols : list of columns to keep
    tz : str e.g. 'US/Eastern'

    Returns
    -------
    dataframe
    """
    # Load data
    df = pd.read_csv(filename)
    df['Time'] = pd.to_datetime(df['Time'], utc=True)
    df.set_index('Time', inplace=True)
    df.columns
    df = df[usecols]
    # Convert the integer index column to timezone-aware datetime
    df = df.tz_convert(tz)

    return df
