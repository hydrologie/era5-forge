from datetime import datetime, timedelta


class Config(object):

    # Bucket configuration
    BUCKET = 's3://era5-atlantic-northeast/netcdf/single-levels/day'
    CLIENT_KWARGS = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                     'region_name': 'us-east-2'}
    CONFIG_KWARGS = {'max_pool_connections': 30}
    PROFILE = 'default'

    STORAGE_OPTIONS = {'profile': PROFILE,
                       'client_kwargs': CLIENT_KWARGS,
                       'config_kwargs': CONFIG_KWARGS
                       }

    # Dataset
    START_DATE = "1979-01-01"
    END_DATE = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%d')

    VARIABLES = {'snow_albedo': 'asn',
                 'convective_available_potential_energy': 'cape',
                 'convective_precipitation': 'cp',
                 'convective_snowfall': 'csf',
                 '2m_dewpoint_temperature': 'd2m',
                 'evaporation': 'e',
                 'large_scale_snowfall': 'lsf',
                 'large_scale_precipitation': 'lsp',
                 'maximum_2m_temperature_since_previous_post_processing': 'mn2t',
                 'minimum_2m_temperature_since_previous_post_processing': 'mx2t',
                 'precipitation_type':'ptype',
                 'runoff':'ro',
                 'snow_depth': 'sd',
                 'snowfall': 'sf',
                 'surface_runoff': 'sro',
                 'surface_net_solar_radiation': 'ssr',
                 '2m_temperature': 't2m',
                 'total_cloud_cover': 'tcc',
                 'total_column_rain_water': 'tcrw',
                 'total_column_water': 'tcw',
                 'total_column_water_vapour': 'tcwv',
                 'total_precipitation': 'tp',
                 'temperature_of_snow_layer': 'tsn',
                 '10m_u_component_of_wind': 'u10',
                 '10m_v_component_of_wind': 'v10'}

    TIMES = ['00:00', '01:00', '02:00',
             '03:00', '04:00', '05:00',
             '06:00', '07:00', '08:00',
             '09:00', '10:00', '11:00',
             '12:00', '13:00', '14:00',
             '15:00', '16:00', '17:00',
             '18:00', '19:00', '20:00',
             '21:00', '22:00', '23:00'
             ]
