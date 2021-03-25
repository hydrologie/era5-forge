import xarray as xr
import cdsapi
from prefect import task, Flow, case, agent
from datetime import datetime, timedelta
import os
import fsspec
import s3fs
import pandas as pd
import itertools
import numpy as np
from itertools import product
from config import Config


def fetch_era5(date, variables_long_name):
    c = cdsapi.Client()

    name = 'reanalysis-era5-single-levels'

    request = {'format': 'netcdf',
               'product_type': 'reanalysis',
               'variable': variables_long_name,
               'area': [63, -96, 40, -52],  # North, West, South, East. Default: global,
               'year': "{:04d}".format(date.year),
               'month': "{:02d}".format(date.month),
               'day': "{:02d}".format(date.day),
               'time': Config.TIMES
               }

    r = c.retrieve(name,
                   request,
                   'tmp.nc')


@task()
def list_available_data_not_in_bucket():
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)

    short_name_variables: list = list(Config.VARIABLES.values())
    date_range: list = pd.date_range(start=Config.START_DATE,
                                     end=Config.END_DATE) \
        .strftime('%Y%m%d')

    all_combinations_filenames: list = ['{}_{}_ERA5_SL_REANALYSIS.nc'.format(date, variable.upper()) for
                                        date, variable in product(date_range, short_name_variables)]
    current_filenames_in_bucket: list = [os.path.basename(filename)
                                         for filename in fs.ls(Config.BUCKET)[1:]]
    missing_filenames_in_bucket: list = list(set(all_combinations_filenames) \
                                             .difference(set(current_filenames_in_bucket)))

    return pd.DataFrame(data=[tuple(filename.split('_')[0:2]) for filename in missing_filenames_in_bucket],
                        columns=['Date', 'Var']).groupby(['Date'])['Var'] \
        .unique() \
        .reset_index() \
        .values


@task(max_retries=5, retry_delay=timedelta(minutes=5))
def save_unique_variable_date_file(dates_vars):

    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)

    chosen_date, variables = dates_vars
    chosen_date = datetime.strptime(chosen_date, '%Y%m%d')
    variables_long_name: list = [var_long_name
                                 for var_long_name, var_short_name in Config.VARIABLES.items()
                                 if var_short_name in list(map(lambda x: x.lower(), variables))]

    fetch_era5(chosen_date, variables_long_name)

    ds = xr.open_mfdataset('tmp.nc')
    if 'expver' in list(ds.dims):
        ds = ds.reduce(np.nansum, 'expver')

    for var in list(variables):
        filename = "{:04d}{:02d}{:02d}_{}_ERA5_SL_REANALYSIS.nc".format(chosen_date.year,
                                                                        chosen_date.month,
                                                                        chosen_date.day,
                                                                        var.upper())

        ds[var.lower()].to_netcdf(filename)
        print(filename)
        fs.put(filename,
               os.path.join(Config.BUCKET,
                            filename))
        os.remove(filename)
    os.remove('tmp.nc')


if __name__ == '__main__':
    with Flow("ERA5-ETL") as flow:
        dates_vars: np.array = list_available_data_not_in_bucket()
        save_unique_variable_date_file.map(dates_vars)

    flow.run()
