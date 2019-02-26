#!/usr/bin/env python
"""
update_dreqs_0129.py

This file adds variable requests for Ofx variables required by ESMValTool.

Adds data requests for two of these for the Met Office models.
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
        {'table_name': 'Ofx',
         'long_name': 'Grid-Cell Area for Ocean Variables',
         'units': 'm2',
         'var_name': 'areacello',
         'standard_name': 'cell_area',
         'cell_methods': 'area: sum',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude ',
         'cmor_name': 'areacello',
         'modeling_realm': 'ocean',
         'frequency': 'fx',
         'cell_measures': '',
         'uid': 'baa3ee94-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'Ofx',
         'long_name': 'Region Selection Index',
         'units': '1',
         'var_name': 'basin',
         'standard_name': 'region',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'integer',
         'dimensions': 'longitude latitude ',
         'cmor_name': 'basin',
         'modeling_realm': 'ocean',
         'frequency': 'fx',
         'cell_measures': 'area: areacello',
         'uid': 'baa3f718-e5dd-11e5-8482-ac72891c3257'},
        {'table_name': 'Ofx',
         'long_name': 'Ocean Model Cell Thickness',
         'units': 'm',
         'var_name': 'thkcello',
         'standard_name': 'cell_thickness',
         'cell_methods': 'area: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude olevel ',
         'cmor_name': 'thkcello',
         'modeling_realm': 'ocean',
         'frequency': 'fx',
         'cell_measures': 'area: areacello volume: volcello',
         'uid': 'bab9bd00-e5dd-11e5-8482-ac72891c3257'},
    ]

    # create the variable requests
    for new_vreq in new_vreqs:
        _vr = get_or_create(VariableRequest, **new_vreq)

    variant_label = 'r1i1p1f1'

    # The standard reference time
    std_units = Settings.get_solo().standard_time_units

    new_dreqs = [
        'areacello_Ofx',
        'basin_Ofx',
    ]

    institute_id = 'MOHC'
    calendar = CALENDAR_360_DAY

    # Institute
    result = match_one(Institute, short_name=institute_id)
    if result:
        institute = result
    else:
        msg = 'institute_id {} not found in the database.'.format(institute_id)
        print(msg)
        raise ValueError(msg)


    # Coupled
    #########

    model_ids = ['HadGEM3-GC31-HH', 'HadGEM3-GC31-HM', 'HadGEM3-GC31-MM',
                 'HadGEM3-GC31-LL']

    experiments = {
        'spinup-1950': {'start_date': datetime(1950, 1, 1),
                         'end_date': datetime(1980, 1, 1)},
        'hist-1950': {'start_date': datetime(1950, 1, 1),
                      'end_date': datetime(2015, 1, 1)},
        'control-1950': {'start_date': datetime(1950, 1, 1),
                         'end_date': datetime(2050, 1, 1)},
        'highres-future': {'start_date': datetime(2015, 1, 1),
                           'end_date': datetime(2051, 1, 1)},
    }

    experiment_objs = []
    for expt in experiments:
        expt_obj = match_one(Experiment, short_name=expt)
        if expt_obj:
            experiment_objs.append(expt_obj)
        else:
            msg = 'experiment {} not found in the database.'.format(expt)
            print(msg)
            raise ValueError(msg)

    # Look up the ClimateModel object for each institute_id  and save the
    # results to a dictionary for quick look up later
    model_objs = []
    for clim_model in model_ids:
        result = match_one(ClimateModel, short_name=clim_model)
        if result:
            model_objs.append(result)
        else:
            msg = ('climate_model {} not found in the database.'.
                   format(clim_model))
            print(msg)
            raise ValueError(msg)

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
                    try:
                        _dr = get_or_create(
                            DataRequest,
                            project=project,
                            institute=institute,
                            climate_model=clim_model,
                            experiment=expt,
                            variable_request=var_req_obj,
                            request_start_time=date2num(
                                experiments[expt.short_name]['start_date'],
                                std_units, calendar
                            ),
                            request_end_time=date2num(
                                experiments[expt.short_name]['end_date'],
                                std_units, calendar
                            ),
                            time_units=std_units,
                            calendar=calendar,
                            rip_code = variant_label
                        )
                    except django.core.exceptions.MultipleObjectsReturned:
                        logger.error('{}'.format(var_req_obj))
                        raise
        else:
            msg = ('Unable to find variable request matching '
                   'cmor_name {} and table_name {} in the '
                   'database.'.format(cmor_name, table_name))
            print(msg)
            raise ValueError(msg)

    # Atmosphere only
    #################

    experiments = {
        'highresSST-present': {'start_date': datetime(1950, 1, 1),
                               'end_date': datetime(2015, 1, 1)},
        'highresSST-future': {'start_date': datetime(2015, 1, 1),
                              'end_date': datetime(2051, 1, 1)},
    }

    model_ids = ['HadGEM3-GC31-HM', 'HadGEM3-GC31-MM', 'HadGEM3-GC31-LM']

    experiment_objs = []
    for expt in experiments:
        expt_obj = match_one(Experiment, short_name=expt)
        if expt_obj:
            experiment_objs.append(expt_obj)
        else:
            msg = 'experiment {} not found in the database.'.format(expt)
            print(msg)
            raise ValueError(msg)

    # Look up the ClimateModel object for each institute_id  and save the
    # results to a dictionary for quick look up later
    model_objs = []
    for clim_model in model_ids:
        result = match_one(ClimateModel, short_name=clim_model)
        if result:
            model_objs.append(result)
        else:
            msg = ('climate_model {} not found in the database.'.
                   format(clim_model))
            print(msg)
            raise ValueError(msg)

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
                    try:
                        _dr = get_or_create(
                            DataRequest,
                            project=project,
                            institute=institute,
                            climate_model=clim_model,
                            experiment=expt,
                            variable_request=var_req_obj,
                            request_start_time=date2num(
                                experiments[expt.short_name]['start_date'],
                                std_units, calendar
                            ),
                            request_end_time=date2num(
                                experiments[expt.short_name]['end_date'],
                                std_units, calendar
                            ),
                            time_units=std_units,
                            calendar=calendar,
                            rip_code = variant_label
                        )
                    except django.core.exceptions.MultipleObjectsReturned:
                        logger.error('{}'.format(var_req_obj))
                        raise
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
