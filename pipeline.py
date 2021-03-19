import fsspec
import xarray as xr
from distributed import Client
import cdsapi
from datetime import date
import os
import shutil

from prefect import task, Flow, case, agent
import prefect
from prefect.schedules import IntervalSchedule
from datetime import date, datetime, timedelta
import urllib.request
import s3fs
import os
import subprocess
import s3fs
import zarr
import pandas as pd
import itertools
from config import Config
# from pipeline.models.hydat import get_available_stations_from_hydat, import_hydat_to_parquet, verify_data_type_exists
# from prefect.utilities.configuration import set_temporary_config
# from pipeline.utils import get_url_paths


@task
def list_files_to_update(dates):

    client_kwargs = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                     'region_name': 'us-east-2'}
    config_kwargs = {'max_pool_connections': 30}

    s3 = s3fs.S3FileSystem(profile='default',
                           client_kwargs=client_kwargs,
                           config_kwargs=config_kwargs)  # public read

    filenames = sorted(set(['{}_{}_ERA5_SL_REANALYSIS.nc'.format(a, b.upper()) for
                a, b in (itertools.product(dates.strftime('%Y%m%d'), list(Config.VARIABLES.values())))]).difference(
        set([os.path.basename(filename) for filename in s3.ls(Config.BUCKET)[1:]])))
    g = [tuple(c.split('_')[0:2]) for c in filenames]
    gb = pd.DataFrame(g, columns=['Date', 'Var']).groupby(['Date'])['Var'].unique()

    return gb.reset_index().values


@task(max_retries=5, retry_delay=timedelta(minutes=5))
def save_files_per_variable(arg):
    chosen_date, variables = arg
    chosen_date = datetime.strptime(chosen_date, '%Y%m%d')
    variables_long_name = [a for a, b in Config.VARIABLES.items() if b in list(map(lambda x: x.lower(), variables))]
    print(chosen_date)
    print(variables_long_name)



    c = cdsapi.Client()

    name = 'reanalysis-era5-single-levels'

    request = {'format': 'netcdf',
               'product_type': 'reanalysis',
               'variable': variables_long_name,
               'area': [63, -96, 40, -52],  # North, West, South, East. Default: global,
               'year': "{:04d}".format(chosen_date.year),
               'month': "{:02d}".format(chosen_date.month),
               'day': "{:02d}".format(chosen_date.day),
               'time': Config.TIMES
               }

    r = c.retrieve(name,
                   request,
                   'tmp.nc')
    # fetch reference data
    # Wasabi cloud storage configurations
    client_kwargs = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                     'region_name': 'us-east-2'}
    config_kwargs = {'max_pool_connections': 30}

    s3 = s3fs.S3FileSystem(profile='default',
                           client_kwargs=client_kwargs,
                           config_kwargs=config_kwargs)  # public read

    ds = xr.open_mfdataset('tmp.nc')

    for var in list(variables):
        filename = "{:04d}{:02d}{:02d}_{}_ERA5_SL_REANALYSIS.nc".format(chosen_date.year,
                                                                        chosen_date.month,
                                                                        chosen_date.day,
                                                                        var.upper())

        print(var.lower())

        ds[var.lower()].to_netcdf(filename)

        s3.put(filename,
               os.path.join(Config.BUCKET,
                            filename))
        os.remove(filename)
    os.remove('tmp.nc')


if __name__ == '__main__':

    with Flow("Hydat-ETL") as flow:
        dates = pd.date_range(start="1979-01-01",
                              end="2021-01-01")

        products = list_files_to_update(dates)
        save_files_per_variable.map(products)

    flow.run()

    # dates = pd.date_range(start="1979-01-01",
    #                       end="2021-01-01")
    #
    # products = list_files_to_update(dates)
    # save_files_per_variable.map(products)


    # # client = Client()
    # # print(client.dashboard_link)
    # client_kwargs = {"endpoint_url": "https://s3.us-east-2.wasabisys.com"}
    #
    # current_year_bucket = 's3://era5-atlantic-northeast/zarr/reanalysis/single-levels/current_year'
    # archive_bucket = 's3://era5-atlantic-northeast/zarr/reanalysis/single-levels/archive'
    #
    # # dates = pd.date_range(start='1979-01-01',
    # #                       end='1979-01-02',
    # #                       freq='1D',
    # #                       normalize=True)
    # dates = []
    # try:
    #     # Look in current year bucket for previous date
    #     # Url du serveur contenant le bucket
    #     store = fsspec.get_mapper(current_year_bucket,
    #                               profile='default',
    #                               client_kwargs=client_kwargs)
    #     # Ouverture du zarr vers dataset (xarray)
    #     ds = xr.open_zarr(store,
    #                       consolidated=True,
    #                       chunks='auto')
    #
    #     dates = pd.date_range(start=ds.time.max().values,
    #                           end=date.today(),
    #                           freq='1D',
    #                           normalize=True)[1:]
    # except Exception:
    #     pass
    #
    # finally:
    #     if not any(dates):
    #         # Look in current year bucket for previous date
    #         # Url du serveur contenant le bucket
    #         store = fsspec.get_mapper(archive_bucket,
    #                                   profile='default',
    #                                   client_kwargs=client_kwargs)
    #         # Ouverture du zarr vers dataset (xarray)
    #         ds = xr.open_zarr(store,
    #                           consolidated=True,
    #                           chunks='auto')
    #
    #         dates = pd.date_range(start=ds.time.max().values,
    #                               end=date.today(),
    #                               freq='1D',
    #                               normalize=True)[1:]
    #
    #
    # # If current year bucket is empty, look into the archive bucket
    #
    # store = fsspec.get_mapper(current_year_bucket,
    #                           profile='default',
    #                           client_kwargs=client_kwargs)
    # for date_time in dates:
    #     ds_out = get_era5(date_time, current_year_bucket, store)
    #     process(ds_out, store)
    #     shutil.rmtree("tmp.zarr")
