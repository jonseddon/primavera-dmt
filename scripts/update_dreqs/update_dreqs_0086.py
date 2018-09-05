#!/usr/bin/env python
"""
update_dreqs_0086.py

This file adds data and variable requests for CERFACS for both models for all
the coupled experiments for variant_label r1i1p1f2.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
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
    new_vreqs = [
        {'table_name': 'Omon',
         'long_name': 'Sea Water Potential Temperature at Sea Floor',
         'units': 'degC',
         'var_name': 'tob',
         'standard_name': 'sea_water_potential_temperature_at_sea_floor',
         'cell_methods': 'area: mean where sea time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'tob',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello',
         'uid': 'baa53218-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'Omon',
         'long_name': 'Sea water Salinity at Sea Floor',
         'units': '0.001',
         'var_name': 'sob',
         'standard_name': 'sea_water_salinity_at_sea_floor',
         'cell_methods': 'area: mean where sea time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'sob',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello',
         'uid': 'baa55f4a-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'Omon',
         'long_name': 'Global Average Sea Surface Salinity',
         'units': '0.001',
         'var_name': 'sosga',
         'standard_name': 'sea_surface_salinity',
         'cell_methods': 'area: mean where sea time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': ' time',
         'cmor_name': 'sosga',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': '',
         'uid': '1aaaf7fe-b006-11e6-9289-ac72891c3257'},
        {'table_name': 'Omon',
         'long_name': 'Maximum Ocean Mixed Layer Thickness Defined by Sigma T',
         'units': 'm',
         'var_name': 'mlotstmax',
         'standard_name': 'ocean_mixed_layer_thickness_defined_by_sigma_t',
         'cell_methods': 'area: mean time: maximum',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'mlotstmax',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello',
         'uid': '1aab3e76-b006-11e6-9289-ac72891c3257'},
        {'table_name': 'Omon',
         'long_name': 'Minimum Ocean Mixed Layer Thickness Defined by Sigma T',
         'units': 'm',
         'var_name': 'mlotstmin',
         'standard_name': 'ocean_mixed_layer_thickness_defined_by_sigma_t',
         'cell_methods': 'area: mean time: minimum',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'mlotstmin',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello',
         'uid': '1aab4e7a-b006-11e6-9289-ac72891c3257'},
        {'table_name': 'Omon',
         'long_name': 'Square of Brunt Vaisala Frequency in Sea Water',
         'units': 's-2',
         'var_name': 'obvfsq',
         'standard_name': 'square_of_brunt_vaisala_frequency_in_sea_water',
         'cell_methods': 'area: mean where sea time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude olevel time',
         'cmor_name': 'obvfsq',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello volume: volcello',
         'uid': '1aab5d20-b006-11e6-9289-ac72891c3257'},
        {'table_name': 'Omon',
         'long_name': 'Ocean Y Overturning Mass Streamfunction',
         'units': 'kg s-1',
         'var_name': 'msftyz',
         'standard_name': 'ocean_y_overturning_mass_streamfunction',
         'cell_methods': 'time: mean grid_longitude: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'latitude olevel basin time',
         'cmor_name': 'msftyz',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': '',
         'uid': 'baa5a662-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': '20C isotherm depth',
         'units': 'm',
         'var_name': 't20d',
         'standard_name': 'depth_of_isosurface_of_sea_water_potential_temperature',
         'cell_methods': 'area: mean where sea time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 't20d',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello',
         'uid': '8b922f7a-4a5b-11e6-9cd2-ac72891c3257'}
    ]

    institute_details = {
        'id': 'CNRM-CERFACS',
        'model_ids': ['CNRM-CM6-1-HR', 'CNRM-CM6-1'],
        'calendar': CALENDAR_GREGORIAN
    }

    experiments = {
        'control-1950': {'start_date': datetime(1950, 1, 1),
                         'end_date': datetime(2050, 1, 1)},
        'highres-future': {'start_date': datetime(2015, 1, 1),
                           'end_date': datetime(2051, 1, 1)},
        'hist-1950': {'start_date': datetime(1950, 1, 1),
                      'end_date': datetime(2015, 1, 1)},
        'spinup-1950': {'start_date': datetime(1950, 1, 1),
                        'end_date': datetime(1980, 1, 1)},
    }

    variant_label = 'r1i1p1f2'

    # Experiment
    new_dreqs = [
        'mlotstmax_Omon',
        'mlotstmin_Omon',
        'msftyz_Omon',
        'obvfsq_Omon',
        'sob_Omon',
        'sosga_Omon',
        't20d_Emon',
        'tob_Omon',
        'agesno_LImon',
        'bigthetaoga_Omon',
        'hfbasin_Omon',
        'hfbasinpmadv_Omon',
        'hurs_day',
        'lwsnl_LImon',
        'mfo_Omon',
        'psl_6hrPlevPt',
        'somint_PrimOmon',
        'tos_3hr',
        'tosga_Omon',
        'uas_6hrPlevPt',
        'vas_6hrPlevPt',
        'wo_Omon'
    ]
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
                        calendar=institute_details['calendar'],
                        rip_code = variant_label
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
