#!/usr/bin/env python2.7
"""
update_dreqs_0007.py

This script is run to add variable and data requests for data that has been 
recently added to the data request spreadsheet.

This file adds data requests for EC-EARTH for the EC-Earth3-HR 
model for the highresSST-present experiment for the v20170619 submission.
"""
import argparse
from datetime import datetime
import logging.config
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

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
    # new_vreqs = ['lwsnl_Eday', 'mrros_Eday', 'snm_Eday', 'snd_Eday']
    new_vreqs = [
        {'table_name': 'Prim6hrPt',
         'long_name': 'Surface Temperature Where Land or Sea Ice',
         'units': 'K',
         'var_name': 'tslsi',
         'standard_name': 'surface_temperature',
         'cell_methods': 'area: mean time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time1',
         'cmor_name': 'tslsi',
         'modeling_realm': 'land',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hrpt_tslsi'},
        {'table_name': 'Prim6hrPt',
         'long_name': 'Sea Surface Temperature',
         'units': 'K',
         'var_name': 'tso',
         'standard_name': 'sea_surface_temperature',
         'cell_methods': 'area: mean where sea time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time1',
         'cmor_name': 'tso',
         'modeling_realm': 'ocean',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hrpt_tso'},
        {'table_name': 'Prim6hrPt',
         'long_name': 'Surface Air Pressure',
         'units': 'Pa',
         'var_name': 'ps',
         'standard_name': 'surface_air_pressure',
         'cell_methods': 'area: mean time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time1',
         'cmor_name': 'ps',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hrpt_ps'},
        {'table_name': 'Prim6hrPt',
         'long_name': 'Total Cloud Fraction',
         'units': '%',
         'var_name': 'clt',
         'standard_name': 'cloud_area_fraction',
         'cell_methods': 'area: time: point',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time1',
         'cmor_name': 'clt',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hrpt_clt'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Upward Latent Heat Flux',
         'units': 'W m-2',
         'var_name': 'hfls',
         'standard_name': 'surface_upward_latent_heat_flux',
         'cell_methods': 'area: time: mean ',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'hfls',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_hfls'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Upward Sensible Heat Flux',
         'units': 'W m-2',
         'var_name': 'hfss',
         'standard_name': 'surface_upward_sensible_heat_flux',
         'cell_methods': 'area: time: mean ',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'hfss',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_hfss'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Downwelling Longwave Radiation',
         'units': 'W m-2',
         'var_name': 'rlds',
         'standard_name': 'surface_downwelling_longwave_flux_in_air',
         'cell_methods': 'area: time: mean ',
         'positive': 'down',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rlds',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_rlds'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Upwelling Longwave Radiation',
         'units': 'W m-2',
         'var_name': 'rlus',
         'standard_name': 'surface_upwelling_longwave_flux_in_air',
         'cell_methods': 'area: time: mean ',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rlus',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_rlus'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Upwelling Shortwave Radiation',
         'units': 'W m-2',
         'var_name': 'rsus',
         'standard_name': 'surface_upwelling_shortwave_flux_in_air',
         'cell_methods': 'area: time: mean ',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rsus',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_rsus'},
        {'table_name': 'Prim6hr',
         'long_name': 'Convective Precipitation',
         'units': 'kg m-2 s-1',
         'var_name': 'prc',
         'standard_name': 'convective_precipitation_flux',
         'cell_methods': 'area: time: mean ',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'prc',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_prc'},
        {'table_name': 'Prim6hr',
         'long_name': 'Snowfall Flux',
         'units': 'kg m-2 s-1',
         'var_name': 'prsn',
         'standard_name': 'snowfall_flux',
         'cell_methods': 'area: time: mean ',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'prsn',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_prsn'},
        {'table_name': 'Prim6hr',
         'long_name': 'Total Runoff',
         'units': 'kg m-2 s-1',
         'var_name': 'mrro',
         'standard_name': 'runoff_flux',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'mrro',
         'modeling_realm': 'land',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_mrro'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Downwelling Clear-Sky Longwave Radiation',
         'units': 'W m-2',
         'var_name': 'rldscs',
         'standard_name': 'surface_downwelling_longwave_flux_in_air_assuming_clear_sky',
         'cell_methods': 'area: time: mean ',
         'positive': 'down',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rldscs',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_rldscs'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Downwelling Clear-Sky Shortwave Radiation',
         'units': 'W m-2',
         'var_name': 'rsdscs',
         'standard_name': 'surface_downwelling_shortwave_flux_in_air_assuming_clear_sky',
         'cell_methods': 'area: time: mean ',
         'positive': 'down',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rsdscs',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_rsdscs'},
        {'table_name': 'Prim6hr',
         'long_name': 'Surface Upwelling Clear-Sky Shortwave Radiation',
         'units': 'W m-2',
         'var_name': 'rsuscs',
         'standard_name': 'surface_upwelling_shortwave_flux_in_air_assuming_clear_sky',
         'cell_methods': 'area: time: mean ',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rsuscs',
         'modeling_realm': 'atmos',
         'frequency': '6hr',
         'cell_measures': 'area: areacella',
         'uid': 'prim6hr_rsuscs'},
        {'table_name': 'PrimOday',
         'long_name': 'Sea Water X Velocity',
         'units': 'm s-1',
         'var_name': 'uo',
         'standard_name': 'sea_water_x_velocity',
         'cell_methods': 'time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude olevel time',
         'cmor_name': 'uo',
         'modeling_realm': 'ocean',
         'frequency': 'day',
         'cell_measures': '',
         'uid': 'primOday_uo'},
        {'table_name': 'PrimOday',
         'long_name': 'Sea Water Y Velocity',
         'units': 'm s-1',
         'var_name': 'vo',
         'standard_name': 'sea_water_y_velocity',
         'cell_methods': 'time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude olevel time',
         'cmor_name': 'vo',
         'modeling_realm': 'ocean',
         'frequency': 'day',
         'cell_measures': '',
         'uid': 'primOday_vo'},
    ]

    new_dreqs = [
        'hfls_Prim6hr',
        'hfss_Prim6hr',
        'rlds_Prim6hr',
        'rlus_Prim6hr',
        'rsus_Prim6hr',
        'prc_Prim6hr',
        'prsn_Prim6hr',
        'mrro_Prim6hr',
        'rldscs_Prim6hr',
        'rsdscs_Prim6hr',
        'rsuscs_Prim6hr',
        'tslsi_Prim6hrPt',
        'tso_Prim6hrPt',
        'ps_Prim6hrPt',
        'clt_Prim6hrPt',
        'uo_PrimOday',
        'vo_PrimOday',
    ]

    institute_details = {
        'id': 'ECMWF',
        'model_ids': ['ECMWF-IFS-HR', 'ECMWF-IFS-LR'],
        'calendar': CALENDAR_GREGORIAN
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
            print msg
            raise ValueError(msg)

    # Institute
    result = match_one(Institute, short_name=institute_details['id'])
    if result:
        institute = result
    else:
        msg = 'institute_id {} not found in the database.'.format(
            institute_details['id']
        )
        print msg
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
            print msg
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
            print msg
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
