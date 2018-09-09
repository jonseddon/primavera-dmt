#!/usr/bin/env python
"""
update_dreqs_0088.py

This file adds data and variable requests for CERFACS for the low res model
for all the coupled experiments for variant_label r2i1p1f2.
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
        {'table_name': 'Eday',
         'long_name': 'River Discharge',
         'units': 'm3 s-1',
         'var_name': 'rivo',
         'standard_name': 'water_flux_to_downstream',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rivo',
         'modeling_realm': 'land',
         'frequency': 'day',
         'cell_measures': 'area: areacellr',
         'uid': 'd2285b46-4a9f-11e6-b84e-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Water table depth',
         'units': 'm',
         'var_name': 'wtd',
         'standard_name': 'depth_of_soil_moisture_saturation',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'wtd',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacellr',
         'uid': '8b81f0ce-4a5b-11e6-9cd2-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Terrestrial Water Storage',
         'units': 'kg m-2',
         'var_name': 'mrtws',
         'standard_name': 'total_water_storage',
         'cell_methods': 'area: mean where land time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'mrtws',
         'modeling_realm': 'land',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f6a4484-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Vertically integrated Eastward moisture transport (Mass_weighted_vertical integral of the product of eastward wind by total water mass per unit mass)',
         'units': 'kg m-1 s-1',
         'var_name': 'intuaw',
         'standard_name': 'vertical_integral_eastward_wind_by_total_water',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'intuaw',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f690484-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Vertically integrated Northward moisture transport (Mass_weighted_vertical integral of the product of northward wind by total water mass per unit mass)',
         'units': 'kg m-1 s-1',
         'var_name': 'intvaw',
         'standard_name': 'vertical_integral_northward_wind_by_total_water',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'intvaw',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f690b5a-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Vertically Integrated Eastward Dry Statice Energy Transport',
         'units': '1.e6 J m-1 s-1',
         'var_name': 'intuadse',
         'standard_name': 'vertical_integral_eastward_wind_by_dry_static_energy',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'intuadse',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f691104-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Vertically integrated Northward dry transport (cp.T +zg).v (Mass_weighted_vertical integral of the product of northward wind by dry static_energy per mass unit)',
         'units': '1.e6 J m-1 s-1',
         'var_name': 'intvadse',
         'standard_name': 'vertical_integral_northward_wind_by_dry_static_energy',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'intvadse',
         'modeling_realm': 'atmos',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '6f6916a4-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'Emon',
         'long_name': 'Heat content of upper 300 meters',
         'units': 'm K',
         'var_name': 'hcont300',
         'standard_name': 'heat_content_of_ocean_layer',
         'cell_methods': 'area: mean where sea time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time depth300m',
         'cmor_name': 'hcont300',
         'modeling_realm': 'ocean',
         'frequency': 'mon',
         'cell_measures': 'area: areacello',
         'uid': '6f69513c-9acb-11e6-b7ee-ac72891c3257'},
        {'table_name': 'AERmon',
         'long_name': 'Sulfate Aerosol Optical Depth at 550nm',
         'units': '1',
         'var_name': 'od550so4',
         'standard_name': 'atmosphere_optical_thickness_due_to_sulfate_ambient_aerosol',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'od550so4',
         'modeling_realm': 'aerosol',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '19bf19ca-81b1-11e6-92de-ac72891c3257'},
        {'table_name': 'AERmon',
         'long_name': 'TOA Outgoing Clear-Sky, Aerosol-Free Shortwave Radiation',
         'units': 'W m-2',
         'var_name': 'rsutcsaf',
         'standard_name': 'toa_outgoing_shortwave_flux_assuming_clear_sky',
         'cell_methods': 'area: time: mean',
         'positive': 'up',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time',
         'cmor_name': 'rsutcsaf',
         'modeling_realm': 'aerosol',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '8feac232-267c-11e7-8933-ac72891c3257'},
        {'table_name': 'SImon',
         'long_name': 'Sea-ice area fraction',
         'units': '%',
         'var_name': 'siconca',
         'standard_name': 'sea_ice_area_fraction',
         'cell_methods': 'area: time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'longitude latitude time typesi',
         'cmor_name': 'siconca',
         'modeling_realm': 'seaIce',
         'frequency': 'mon',
         'cell_measures': 'area: areacella',
         'uid': '71190054-faa7-11e6-bfb7-ac72891c3257'},
        {'table_name': 'SImon',
         'long_name': 'Sea ice area flux through straits',
         'units': 'm2 s-1',
         'var_name': 'siareaacrossline',
         'standard_name': 'sea_ice_area_transport_across_line',
         'cell_methods': 'time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'siline time',
         'cmor_name': 'siareaacrossline',
         'modeling_realm': 'seaIce',
         'frequency': 'mon',
         'cell_measures': '',
         'uid': '712442ca-faa7-11e6-bfb7-ac72891c3257'},
        {'table_name': 'SImon',
         'long_name': 'Snow mass flux through straits',
         'units': 'kg s-1',
         'var_name': 'snmassacrossline',
         'standard_name': 'snow_mass_transport_across_line',
         'cell_methods': 'time: mean',
         'positive': '',
         'variable_type': 'real',
         'dimensions': 'siline time',
         'cmor_name': 'snmassacrossline',
         'modeling_realm': 'seaIce',
         'frequency': 'mon',
         'cell_measures': '',
         'uid': '712fb3ee-faa7-11e6-bfb7-ac72891c3257'},
    ]

    institute_details = {
        'id': 'CNRM-CERFACS',
        'model_ids': ['CNRM-CM6-1'],
        'calendar': CALENDAR_GREGORIAN
    }

    experiments = {
        'control-1950': {'start_date': datetime(1950, 1, 1),
                         'end_date': datetime(2050, 1, 1)},
        'highres-future': {'start_date': datetime(2015, 1, 1),
                           'end_date': datetime(2051, 1, 1)},
        'hist-1950': {'start_date': datetime(1950, 1, 1),
                      'end_date': datetime(2015, 1, 1)},
    }

    variant_label = 'r2i1p1f2'

    # Experiment
    new_dreqs = [
        'rivo_Eday',
        'siforceintstrx_PrimSIday',
        'siforceintstry_PrimSIday',
        'simassacrossline_PrimSIday',
        'hfgeou_Omon',
        'htovgyre_Omon',
        'htovovrt_Omon',
        'sicompstren_SImon',
        'siflcondbot_SImon',
        'siflcondtop_SImon',
        'siflfwdrain_SImon',
        'sifllatstop_SImon',
        'sifllwutop_SImon',
        'siflsensupbot_SImon',
        'siflswdtop_SImon',
        'siflswutop_SImon',
        'simassacrossline_SImon',
        'sisnmass_SImon',
        'sitempbot_SImon',
        'sithick_SImon',
        'sltovgyre_Omon',
        'sltovovrt_Omon',
        'hcont300_Emon',
        'intuadse_Emon',
        'intuaw_Emon',
        'intvadse_Emon',
        'intvaw_Emon',
        'mrtws_Emon',
        'od550so4_AERmon',
        'rsutcsaf_AERmon',
        'siareaacrossline_SImon',
        'siconca_SImon',
        'snmassacrossline_SImon',
        'wtd_Emon',
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
