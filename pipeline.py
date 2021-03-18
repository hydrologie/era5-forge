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


# @task
# def update_hydat_database(path):
#     project_root = '/tmp'
#     data_dir = os.path.join(project_root, 'data')
#     stations_list = get_available_stations_from_hydat()
#     #
#     # results = []
#     for station_number in stations_list:
#         if verify_data_type_exists(station_number, 'Flow'):
#             import_hydat_to_parquet(station_number)
#
#     storage_options = {"client_kwargs": {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
#                                          'region_name': 'us-east-2'}}
#
#     df = pd.read_parquet(os.path.join(data_dir, 'basin.parquet'), engine='pyarrow')
#     df.to_parquet('s3://hydrology/timeseries/sources/hydat/basin.parquet',
#                   engine='fastparquet',
#                   compression='gzip',
#                   storage_options=storage_options)
#     df = pd.read_parquet(os.path.join(data_dir, 'context.parquet'), engine='pyarrow')
#     df.to_parquet('s3://hydrology/timeseries/sources/hydat/context.parquet',
#                   engine='fastparquet',
#                   compression='gzip',
#                   storage_options=storage_options)
#
#     client_kwargs = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
#                      'region_name': 'us-east-2'}
#     config_kwargs = {'max_pool_connections': 30}
#
#     bucket_source = os.path.join(data_dir, 'zarr')
#     bucket_sink = "s3://hydrology/timeseries/sources/hydat/values.zarr "
#     endpoint_url = 'https://s3.us-east-2.wasabisys.com'
#     region='us-east-2'
#
#     s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs,
#                            config_kwargs=config_kwargs)
#     store = s3fs.S3Map(root=bucket_sink,
#                        s3=s3)
#
#
#     aws_command = "aws s3 sync {} {} --endpoint-url={} --region={}".format(bucket_source,
#                                                                            bucket_sink,
#                                                                            endpoint_url,
#                                                                            region)
#     print(aws_command)
#     subprocess.call(aws_command, shell=True)
#
#     zarr.consolidate_metadata(store)
#
#
# # schedule to run every 12 hours
# schedule = IntervalSchedule(
#     start_date=datetime.utcnow() + timedelta(seconds=1),
#     interval=timedelta(hours=12),
#     end_date=datetime.utcnow() + timedelta(seconds=20))
#
# # import pendulum
# #
# # from prefect.schedules import Schedule
# # from prefect.schedules.clocks import DatesClock
# #
# # schedule = Schedule(
# #     clocks=[DatesClock([pendulum.now().add(seconds=1)])])
#
# temp_config = {
#     "cloud.agent.auth_token": prefect.config.cloud.agent.auth_token,
# }


# @retry(stop_max_attempt_number=10)
# def get_era5(date, bucket, store):
#     year = "{:04d}".format(date.year)
#     month = "{:02d}".format(date.month)
#     day = "{:02d}".format(date.day)
#
#     c = cdsapi.Client(verify=False)
#
#     name = 'reanalysis-era5-single-levels'
#     request = {'format': 'netcdf',
#                'product_type': 'reanalysis',
#                'variable': [
#                    'snow_albedo',
#                    'convective_available_potential_energy',
#                    'convective_precipitation',
#                    'convective_snowfall',
#                    '2m_dewpoint_temperature',
#                    'evaporation',
#                    'large_scale_snowfall',
#                    'large_scale_precipitation',
#                    'maximum_2m_temperature_since_previous_post_processing',
#                    'minimum_2m_temperature_since_previous_post_processing',
#                    'precipitation_type',
#                    'potential_vorticity',
#                    'runoff',
#                    'snow_depth',
#                    'snowfall',
#                    'surface_runoff',
#                    'surface_net_solar_radiation',
#                    '2m_temperature',
#                    'total_cloud_cover',
#                    'total_column_rain_water',
#                    'total_column_water',
#                    'total_column_water_vapour',
#                    'total_precipitation',
#                    'temperature_of_snow_layer',
#                    '10m_u_component_of_wind',
#                    '10m_v_component_of_wind',
#                ],
#                'area': [63, -96, 40, -52],  # North, West, South, East. Default: global,
#                'year': year,
#                'month': [
#                    month
#                ],
#                'day': [
#                    day
#                ],
#                'time': [
#                    '00:00', '01:00', '02:00',
#                    '03:00', '04:00', '05:00',
#                    '06:00', '07:00', '08:00',
#                    '09:00', '10:00', '11:00',
#                    '12:00', '13:00', '14:00',
#                    '15:00', '16:00', '17:00',
#                    '18:00', '19:00', '20:00',
#                    '21:00', '22:00', '23:00'
#                ]
#                }
#
#     r = c.retrieve(name,
#                    request,
#                    'tmp.nc')
#     ds_out = xr.open_dataset('tmp.nc', engine='scipy')
#
#     if 'expver' in list(ds_out.dims):
#         ds_out = ds_out.sel(expver=5).where(~ds_out.sel(expver=5, drop=True).isnull(),
#                                             ds_out.sel(expver=1, drop=True))
#     #         for name in list(ds_out.keys()):
#     #             del ds_out[name].encoding['chunks']
#     return ds_out
#
# @retry(stop_max_attempt_number=10)
# def process(ds_out, store):
#     print('Chunking dataset...')
#     ds_out = ds_out.chunk({'longitude': 50, 'latitude': 50})
#
#     print('Saving to zarr...')
#     ds_out.to_zarr('tmp.zarr', consolidated=True)
#
#     print('Adding new zarr data to existing dataset...')
#     ds_add = xr.open_zarr('tmp.zarr', consolidated=True)
#
#     print(any(store.fs.glob(os.path.join(current_year_bucket, '*'))))
#     if not any(store.fs.glob(os.path.join(current_year_bucket, '*'))):
#         ds_add.to_zarr(store, consolidated=True)
#     else:
#         ds_add.to_zarr(store, mode='a',
#                        append_dim='time', consolidated=True)


if __name__ == '__main__':
    with Flow("Hydat-ETL") as flow:
        # with set_temporary_config(temp_config):
        #     if flow.run_config is not None:
        #         labels = list(flow.run_config.labels or ())
        #     elif flow.environment is not None:
        #         labels = list(flow.environment.labels or ())
        #     else:
        #         labels = []
        #     agent = agent.local.LocalAgent(
        #         labels=labels, max_polls=50
        #     )
        dates = pd.date_range(start="1979-01-01",
                              end="2021-01-01")

        products = list_files_to_update(dates)
        save_files_per_variable.map(products)

            # cond = verify_if_to_date(path)
            # with case(cond, False):
            #     path = download_hydat_file(path)
            #     update_hydat_database(path)

    # flow.register(project_name="hydat-file-upload")
    # agent.start()
    flow.run()


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
