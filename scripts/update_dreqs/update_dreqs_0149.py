#!/usr/bin/env python
"""
update_dreqs_0149.py

This file adds data requests for CERFACS for the AMIP models.
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
    institute_details = {
        'id': 'CNRM-CERFACS',
        'model_ids': ['CNRM-CM6-1', 'CNRM-CM6-1-HR'],
        'calendar': CALENDAR_GREGORIAN
    }

    experiments = {
        'highresSST-present': {'start_date': datetime(1950, 1, 1),
                               'end_date': datetime(2015, 1, 1)},
        'highresSST-future': {'start_date': datetime(2015, 1, 1),
                              'end_date': datetime(2051, 1, 1)},
    }

    variant_label = 'r1i1p1f2'

    new_vreqs = [
        {'table_name': 'EmonZ',
         'long_name': 'v-tendency nonorographic gravity wave drag',
         'units': 'm s-2',
         'var_name': 'vtendnogw',
         'standard_name': 'tendency_of_northward_wind_due_to_nonorographic_gravity_wave_drag',
         'cell_methods': 'longitude: mean time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'latitude plev39 time',
         'cmor_name': 'vtendnogw',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': '',
         'uid': '8183eac8-f906-11e6-a176-5404a60d96b5'},
        {'table_name': 'Emon',
         'long_name': 'C3 grass Area Percentage',
         'units': '%',
         'var_name': 'grassFracC3',
         'standard_name': 'area_fraction',
         'cell_methods': 'area: mean where land over all_area_types time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typec3pft typenatgr',
         'cmor_name': 'grassFracC3',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8b814764-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'C4 grass Area Percentage',
         'units': '%',
         'var_name': 'grassFracC4',
         'standard_name': 'area_fraction',
         'cell_methods': 'area: mean where land over all_area_types time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typec4pft typenatgr',
         'cmor_name': 'grassFracC4',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8b814cc8-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Percentage of land use tile tile that is non-woody vegetation ( e.g. herbaceous crops)',
         'units': '%',
         'var_name': 'nwdFracLut',
         'standard_name': 'missing',
         'cell_methods': 'area: mean where land over all_area_types time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude landUse time typenwd',
         'cmor_name': 'nwdFracLut',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': 'd22da9f2-4a9f-11e6-b84e-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Total vegetated fraction',
         'units': '%',
         'var_name': 'vegFrac',
         'standard_name': 'area_fraction',
         'cell_methods': 'area: mean where land over all_area_types time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typeveg',
         'cmor_name': 'vegFrac',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f6a57d0-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Percentage Cover by C3 Crops',
         'units': '%',
         'var_name': 'cropFracC3',
         'standard_name': 'area_fraction',
         'cell_methods': 'area: mean where land over all_area_types time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typec3pft typecrop',
         'cmor_name': 'cropFracC3',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8b81522c-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Percentage Cover by C4 Crops',
         'units': '%',
         'var_name': 'cropFracC4',
         'standard_name': 'area_fraction',
         'cell_methods': 'area: mean where land over all_area_types time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typec4pft typecrop',
         'cmor_name': 'cropFracC4',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f6a8ea8-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'LImon',
         'long_name': 'Land Ice Area Percentage',
         'units': '%',
         'var_name': 'sftgif',
         'standard_name': 'land_ice_area_fraction',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typeli',
         'cmor_name': 'sftgif',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': 'b3d6e26a-3578-11e7-8257-5404a60d96b5'},
        {'table_name': 'LImon',
         'long_name': 'Grounded Ice Sheet Area Percentage',
         'units': '%',
         'var_name': 'sftgrf',
         'standard_name': 'grounded_ice_sheet_area_fraction',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typegis',
         'cmor_name': 'sftgrf',
         'modeling_realm': 'landIce',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8bbb1520-4a5b-11e6-9cd2-ac72891c3257'},
    ]

    # Experiment
    new_dreqs = [
        'vtendnogw_EmonZ',
        'cropFracC3_Emon',
        'cropFracC4_Emon',
        'grassFracC3_Emon',
        'grassFracC4_Emon',
        'nwdFracLut_Emon',
        'ps_AERmon',
        'sftgif_LImon',
        'sftgrf_LImon',
        'sidconcdyn_SImon',
        'sidconcth_SImon',
        'simass_SImon',
        'sisaltmass_SImon',
        'sisnthick_SImon',
        'sndmassdyn_SImon',
        'sndmasssi_SImon',
        'vegFrac_Emon',
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
