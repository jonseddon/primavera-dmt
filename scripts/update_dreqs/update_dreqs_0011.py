#!/usr/bin/env python2.7
"""
update_dreqs_0011.py

This script is run to add variable for data that was added to the data request
between versions 01.00.07 and 01.00.13 to ensure compatibility with HighResMIP.

This file adds data requests for HadGEM3 models.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
from datetime import datetime
import logging.config
import sys

from cf_units import date2num, CALENDAR_360_DAY

import django
django.setup()

from pdata_app.models import (DataRequest, VariableRequest, Experiment,
                              Institute, ClimateModel, Project, Settings)
from pdata_app.utils.dbapi import match_one, get_or_create


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)



def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add additional data requests')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    new_vreqs = [
        {'table_name': 'AERmon',
         'long_name': 'Convective Cloud Area Fraction',
         'units': '%',
         'var_name': 'cltc',
         'standard_name': 'convective_cloud_area_fraction',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'float',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'cltc',
         'modeling_realm': 'aerosol',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '01d412d0-c792-11e6-aa58-5404a60d96b5'},
        {'table_name': 'AERmon',
         'long_name': 'Eastward Wind',
         'units': 'm s-1',
         'var_name': 'ua',
         'standard_name': 'eastward_wind',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'float',
         'dimensions': 'longitude latitude alevel time',
         'cmor_name': 'ua',
         'modeling_realm': 'aerosol',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '19bfc73a-81b1-11e6-92de-ac72891c3257'},
        {'table_name': 'AERmon',
         'long_name': 'Northward Wind',
         'units': 'm s-1',
         'var_name': 'va',
         'standard_name': 'northward_wind',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'float',
         'dimensions': 'longitude latitude alevel time',
         'cmor_name': 'va',
         'modeling_realm': 'aerosol',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '19bfc9f6-81b1-11e6-92de-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Specific Humidity',
         'units': '1.0',
         'var_name': 'hus',
         'standard_name': 'specific_humidity',
         'cell_methods': 'time: mean',
         'positive': '',
         'variable_type': '',
         'dimensions': 'longitude latitude plev7c time',
         'cmor_name': 'hus',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '81d83384-aa6a-11e6-9736-5404a60d96b5'},
        {'table_name': 'E3hr',
         'long_name': 'Sea Level Pressure',
         'units': 'Pa',
         'var_name': 'psl',
         'standard_name': 'air_pressure_at_sea_level',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'psl',
         'modeling_realm': 'atmos',
         'frequency': '3hr',
         'cell_measures': 'area: areacella',
         'uid': '8bb0d1c8-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'E3hrPt',
         'long_name': 'ISCCP Percentage Cloud Area',
         'units': '%',
         'var_name': 'clisccp',
         'standard_name': 'cloud_area_fraction_in_atmosphere_layer',
         'cell_methods': 'area: mean time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude plev7c tau time1',
         'cmor_name': 'clisccp',
         'modeling_realm': 'atmos',
         'frequency': '3hr',
         'cell_measures': 'area: areacella',
         'uid': '8bb0db0a-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': '6hrPlevPt',
         'long_name': 'Geopotential Height at 500 hPa',
         'units': 'm',
         'var_name': 'zg500',
         'standard_name': 'geopotential_height',
         'cell_methods': 'area: mean time: point',
         'positive': '',
         'variable_type': 'float',
         'dimensions': 'longitude latitude time1',
         'cmor_name': 'zg500',
         'modeling_realm': 'aerosol',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '7c70f59e-1ab7-11e7-8dfc-5404a60d96b5'},
        {'table_name': '6hrPlevPt',
         'long_name': 'Eastward Wind',
         'units': 'm s-1',
         'var_name': 'ua',
         'standard_name': 'eastward_wind',
         'cell_methods': 'area: mean time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude plev3 time1',
         'cmor_name': 'ua',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '8bae55ba-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': '6hrPlevPt',
         'long_name': 'Northward Wind',
         'units': 'm s-1',
         'var_name': 'va',
         'standard_name': 'northward_wind',
         'cell_methods': 'area: mean time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude plev3 time1',
         'cmor_name': 'va',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '8bae5aba-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': '6hrPlevPt',
         'long_name': 'Air Temperature',
         'units': 'K',
         'var_name': 'ta',
         'standard_name': 'air_temperature',
         'cell_methods': 'area: mean time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude plev3 time1',
         'cmor_name': 'ta',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '6a35d178-aa6a-11e6-9736-5404a60d96b5'},
        {'table_name': 'Eday',
         'long_name': 'Liquid Water Content of Snow Layer',
         'units': 'kg m-2',
         'var_name': 'lwsnl',
         'standard_name': 'liquid_water_content_of_snow_layer',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'lwsnl',
         'modeling_realm': 'landIce land',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': 'd228925a-4a9f-11e6-b84e-ac72891c3257'},
        {'table_name': 'Eday',
         'long_name': 'Surface Runoff',
         'units': 'kg m-2 s-1',
         'var_name': 'mrros',
         'standard_name': 'surface_runoff_flux',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'mrros',
         'modeling_realm': 'land',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': 'd2284048-4a9f-11e6-b84e-ac72891c3257'},
        {'table_name': 'Eday',
         'long_name': 'Surface Snow Melt',
         'units': 'kg m-2 s-1',
         'var_name': 'snm',
         'standard_name': 'surface_snow_melt_flux',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'snm',
         'modeling_realm': 'landIce land',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': 'd22848ea-4a9f-11e6-b84e-ac72891c3257'},
        {'table_name': 'Eday',
         'long_name': 'Snow Depth',
         'units': 'm',
         'var_name': 'snd',
         'standard_name': 'surface_snow_thickness',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'snd',
         'modeling_realm': 'landIce land',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': 'b7ccdf0a-7c00-11e6-bcdf-ac72891c3257'},
        {'table_name': 'Eday',
         'long_name': 'Surface Temperature',
         'units': 'K',
         'var_name': 'ts',
         'standard_name': 'surface_temperature',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'ts',
         'modeling_realm': 'atmos',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '8b8fc3de-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Eday',
         'long_name': 'Eastward Wind',
         'units': 'm s-1',
         'var_name': 'ua',
         'standard_name': 'eastward_wind',
         'cell_methods': 'time: mean',
         'positive': '',
         'variable_type': '',
         'dimensions': 'longitude latitude plev19 time',
         'cmor_name': 'ua',
         'modeling_realm': 'atmos',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '8bae00ba-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Eday',
         'long_name': 'Northward Wind',
         'units': 'm s-1',
         'var_name': 'va',
         'standard_name': 'northward_wind',
         'cell_methods': 'time: mean',
         'positive': '',
         'variable_type': '',
         'dimensions': 'longitude latitude plev19 time',
         'cmor_name': 'va',
         'modeling_realm': 'atmos',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '8bae05ec-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': '6hrPlev',
         'long_name': 'Specific Humidity',
         'units': '1.0',
         'var_name': 'hus',
         'standard_name': 'specific_humidity',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': '',
         'dimensions': 'longitude latitude plev4 time',
         'cmor_name': 'hus4',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': '8bae64ba-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'day',
         'long_name': 'Total Soil Moisture Content',
         'units': 'kg m-2',
         'var_name': 'mrso',
         'standard_name': 'soil_moisture_content',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'mrso',
         'modeling_realm': 'land',
         'frequency': 'day',
         'cell_measures': 'area: areacella',
         'uid': '3c641b6c-b89b-11e6-be04-ac72891c3257'},
    ]

    new_dreqs = [
        'psl_E3hr',
    ]

    institute_details = {
        'id': 'MOHC',
        'model_ids': ['HadGEM3-GC31-HM', 'HadGEM3-GC31-MM', 'HadGEM3-GC31-LM'],
        'calendar': CALENDAR_360_DAY
    }

    experiments = {
        'control-1950': {'start_date': datetime(1950, 1, 1),
                         'end_date': datetime(2050, 1, 1)},
        'highres-future': {'start_date': datetime(2015, 1, 1),
                           'end_date': datetime(2051, 1, 1)},
        'hist-1950': {'start_date': datetime(1950, 1, 1),
                      'end_date': datetime(2015, 1, 1)},
        'highresSST-present': {'start_date': datetime(1950, 1, 1),
                               'end_date': datetime(2015, 1, 1)},
        'highresSST-future': {'start_date': datetime(2015, 1, 1),
                              'end_date': datetime(2051, 1, 1)},
        'highresSST-LAI': {'start_date': datetime(1950, 1, 1),
                           'end_date': datetime(2015, 1, 1)},
        'highresSST-smoothed': {'start_date': datetime(1950, 1, 1),
                                'end_date': datetime(2015, 1, 1)},
        'highresSST-p4K': {'start_date': datetime(1950, 1, 1),
                           'end_date': datetime(2015, 1, 1)},
        'highresSST-4co2': {'start_date': datetime(1950, 1, 1),
                            'end_date': datetime(2015, 1, 1)},
        'spinup-1950': {'start_date': datetime(1950, 1, 1),
                        'end_date': datetime(1980, 1, 1)},
    }

    # Experiment
    experiment_objs = []
    for expt in experiments:
        expt_obj = match_one(Experiment, short_name=expt)
        if expt_obj:
            experiment_objs.append(expt_obj)
        else:
            msg = 'experiment {} not found in the database.'.format(expt)
            print(msg)
            raise ValueError(msg)

    # Institute
    result = match_one(Institute, short_name=institute_details['id'])
    if result:
        institute = result
    else:
        msg = 'institute_id {} not found in the database.'.format(
            institute_details['id']
        )
        print(msg)
        raise ValueError(msg)

    # Look up the ClimateModel object for each institute_id  and save the
    # results to a dictionary for quick look up later
    model_objs = []
    for clim_model in institute_details['model_ids']:
        result = match_one(ClimateModel, short_name=clim_model)
        if result:
            model_objs.append(result)
        else:
            msg = ('climate_model {} not found in the database.'.
                   format(clim_model))
            print(msg)
            raise ValueError(msg)

    # The standard reference time
    std_units = Settings.get_solo().standard_time_units

    # create the additional variable requests
    for new_vreq in new_vreqs:
        _vr = get_or_create(VariableRequest, **new_vreq)

    # create the new data requests
    for new_dreq in new_dreqs:
        cmor_name, table_name = new_dreq.split('_')
        if table_name.startswith('Prim'):
            project = match_one(Project, short_name='PRIMAVERA')
        else:
            project = match_one(Project, short_name='CMIP6')

        var_req_obj = match_one(VariableRequest, cmor_name=cmor_name,
                                table_name=table_name)
        if var_req_obj:
            for expt in experiment_objs:
                for clim_model in model_objs:
                    _dr = get_or_create(
                        DataRequest,
                        project=project,
                        institute=institute,
                        climate_model=clim_model,
                        experiment=expt,
                        variable_request=var_req_obj,
                        request_start_time=date2num(
                            experiments[expt.short_name]['start_date'],
                            std_units, institute_details['calendar']
                        ),
                        request_end_time=date2num(
                            experiments[expt.short_name]['end_date'],
                            std_units, institute_details['calendar']
                        ),
                        time_units=std_units,
                        calendar=institute_details['calendar']
                    )
        else:
            msg = ('Unable to find variable request matching '
                   'cmor_name {} and table_name {} in the '
                   'database.'.format(cmor_name, table_name))
            print(msg)
            raise ValueError(msg)


if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    if cmd_args.log_level:
        try:
            log_level = getattr(logging, cmd_args.log_level.upper())
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        log_level = DEFAULT_LOG_LEVEL

    # configure the logger
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': DEFAULT_LOG_FORMAT,
            },
        },
        'handlers': {
            'default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True
            }
        }
    })

    # run the code
    main(cmd_args)
