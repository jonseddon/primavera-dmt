#!/usr/bin/env python
"""
update_dreqs_0269.py

This file creates additional data requests for the ECMWF contribution to WP5
with experiment name primWP5-coupledseaice.
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
                              Institute, ClimateModel, Project, Settings,
                              ActivityId)
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
    activity_id = 'primWP5'

    new_dreqs = [
        'psl_6hrPlevPt',
        'tas_6hrPlevPt',
        'uas_6hrPlevPt',
        'vas_6hrPlevPt',
        'clivi_Amon',
        'clt_Amon',
        'clwvi_Amon',
        'evspsbl_Amon',
        'hfls_Amon',
        'hfss_Amon',
        'hur_Amon',
        'hus_Amon',
        'pr_Amon',
        'prc_Amon',
        'prsn_Amon',
        'prw_Amon',
        'ps_Amon',
        'psl_Amon',
        'rlds_Amon',
        'rldscs_Amon',
        'rlus_Amon',
        'rlut_Amon',
        'rlutcs_Amon',
        'rsds_Amon',
        'rsdscs_Amon',
        'rsdt_Amon',
        'rsus_Amon',
        'rsuscs_Amon',
        'rsut_Amon',
        'rsutcs_Amon',
        'sfcWind_Amon',
        'ta_Amon',
        'tas_Amon',
        'tasmax_Amon',
        'tasmin_Amon',
        'tauu_Amon',
        'tauv_Amon',
        'ts_Amon',
        'ua_Amon',
        'uas_Amon',
        'va_Amon',
        'vas_Amon',
        'wap_Amon',
        'zg_Amon',
        'snd_LImon',
        'snm_LImon',
        'snw_LImon',
        'tsn_LImon',
        'lai_Lmon',
        'mrlsl_Lmon',
        'mrro_Lmon',
        'mrso_Lmon',
        'tsl_Lmon',
        'sos_Oday',
        'tos_Oday',
        'hfds_Omon',
        'mlotst_Omon',
        'mlotstsq_Omon',
        'so_Omon',
        'sos_Omon',
        'tauuo_Omon',
        'tauvo_Omon',
        'thetao_Omon',
        'tos_Omon',
        'tossq_Omon',
        'uo_Omon',
        'vo_Omon',
        'wfo_Omon',
        'wo_Omon',
        'zos_Omon',
        'zossq_Omon',
        'clt_Prim6hrPt',
        'mlotst_PrimOday',
        'tauuo_PrimOday',
        'tauvo_PrimOday',
        'uo_PrimOday',
        'vo_PrimOday',
        'zos_PrimOday',
        'uso_PrimOmon',
        'uto_PrimOmon',
        'vso_PrimOmon',
        'vto_PrimOmon',
        'wso_PrimOmon',
        'wto_PrimOmon',
        'sistrxdtop_PrimSIday',
        'sistrydtop_PrimSIday',
        'evspsbl_Primday',
        'mrlsl_Primday',
        'mrso_Primday',
        'ts_Primday',
        'uneutrals_Primday',
        'vneutrals_Primday',
        'siconc_SIday',
        'sisnthick_SIday',
        'sitemptop_SIday',
        'sithick_SIday',
        'siu_SIday',
        'siv_SIday',
        'siconc_SImon',
        'sisnthick_SImon',
        'sistrxdtop_SImon',
        'sistrydtop_SImon',
        'sitemptop_SImon',
        'sithick_SImon',
        'siu_SImon',
        'siv_SImon',
        'sivol_SImon',
        'clt_day',
        'hfls_day',
        'hfss_day',
        'hur_day',
        'hus_day',
        'pr_day',
        'prc_day',
        'prsn_day',
        'psl_day',
        'rlds_day',
        'rlus_day',
        'rlut_day',
        'rsds_day',
        'rsus_day',
        'sfcWind_day',
        'tas_day',
        'tasmax_day',
        'tasmin_day',
        'tslsi_day',
        'uas_day',
        'vas_day',
        'wap_day',
    ]

    institute_details = {
        'id': 'ECMWF',
        'model_ids': ['ECMWF-IFS-LR', 'ECMWF-IFS-HR'],
        'calendar': CALENDAR_GREGORIAN
    }

    experiments = {
        'primWP5-coupledseaice': {'start_date': datetime(1950, 1, 1),
                                  'end_date': datetime(1960, 1, 1)},
    }

    variant_labels = ['r{}i1p1f1'.format(i) for i in range(1, 41)]

    # activity_id
    ActivityId.objects.get_or_create(short_name=activity_id,
                                     full_name=activity_id)

    # Experiment create
    for expt in experiments:
        Experiment.objects.get_or_create(short_name=expt, full_name=expt)
    # Experiment cache
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
                    for var_lab in variant_labels:
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
                            rip_code = var_lab
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
