#!/usr/bin/env python
"""
update_dreqs_0156.py

This file corrects data requests for CERFACS' contributions to WP5.
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
    # delete the existing requests made by accident
    drs = DataRequest.objects.filter(
        institute__short_name='CNRM-CERFACS',
        experiment__short_name__in=['primWP5-amv-neg', 'primWP5-amv-pos'],
    )
    logger.debug('{} CERFACS WP5 data requests found'.format(drs.count()))
    drs.delete()

    # remake them with the correct variant labels

    activity_id = 'primWP5'

    new_dreqs = [
        'uas_3hr',
        'vas_3hr',
        'pr_6hrPlev',
        'psl_6hrPlev',
        'tas_6hrPlev',
        'uas_6hrPlev',
        'vas_6hrPlev',
        'zg1000_6hrPlev',
        'ta_6hrPlevPt',
        'ua_6hrPlevPt',
        'va_6hrPlevPt',
        'zg27_6hrPlevPt',
        'zg500_AERday',
        'ptp_AERmon',
        'rlutaf_AERmon',
        'rlutcsaf_AERmon',
        'rsutaf_AERmon',
        'rsutcsaf_AERmon',
        'toz_AERmon',
        'ztp_AERmon',
        'clivi_Amon',
        'clt_Amon',
        'clwvi_Amon',
        'evspsbl_Amon',
        'hfls_Amon',
        'hfss_Amon',
        'hur_Amon',
        'hurs_Amon',
        'hus_Amon',
        'huss_Amon',
        'o3_Amon',
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
        'sbl_Amon',
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
        'ps_CFday',
        'mlotst_Eday',
        'rivo_Eday',
        't20d_Eday',
        'ta850_Eday',
        'ts_Eday',
        'sftgrf_Efx',
        'cropFracC3_Emon',
        'cropFracC4_Emon',
        'evspsblpot_Emon',
        'grassFracC3_Emon',
        'grassFracC4_Emon',
        'intuadse_Emon',
        'intuaw_Emon',
        'intvadse_Emon',
        'intvaw_Emon',
        'mrtws_Emon',
        'nwdFracLut_Emon',
        'orog_Emon',
        'rls_Emon',
        'rss_Emon',
        'sfcWindmax_Emon',
        't20d_Emon',
        'thetaot_Emon',
        'thetaot2000_Emon',
        'thetaot300_Emon',
        'thetaot700_Emon',
        'vegFrac_Emon',
        'wtd_Emon',
        'lwsnl_LImon',
        'sftgif_LImon',
        'sftgrf_LImon',
        'snc_LImon',
        'snd_LImon',
        'snw_LImon',
        'baresoilFrac_Lmon',
        'c3PftFrac_Lmon',
        'c4PftFrac_Lmon',
        'cropFrac_Lmon',
        'gpp_Lmon',
        'grassFrac_Lmon',
        'lai_Lmon',
        'mrfso_Lmon',
        'mrro_Lmon',
        'mrros_Lmon',
        'mrso_Lmon',
        'mrsos_Lmon',
        'npp_Lmon',
        'prveg_Lmon',
        'ra_Lmon',
        'rh_Lmon',
        'treeFrac_Lmon',
        'tsl_Lmon',
        'sos_Oday',
        'tos_Oday',
        'areacello_Ofx',
        'basin_Ofx',
        'deptho_Ofx',
        'hfgeou_Ofx',
        'masscello_Ofx',
        'thkcello_Ofx',
        'bigthetao_Omon',
        'bigthetaoga_Omon',
        'ficeberg_Omon',
        'friver_Omon',
        'hfbasin_Omon',
        'hfcorr_Omon',
        'hfds_Omon',
        'hfx_Omon',
        'hfy_Omon',
        'htovgyre_Omon',
        'htovovrt_Omon',
        'mfo_Omon',
        'mlotst_Omon',
        'mlotstmin_Omon',
        'msftyz_Omon',
        'rsntds_Omon',
        'so_Omon',
        'soga_Omon',
        'sos_Omon',
        'sosga_Omon',
        'tauuo_Omon',
        'tauvo_Omon',
        'thetao_Omon',
        'thkcello_Omon',
        'tos_Omon',
        'umo_Omon',
        'uo_Omon',
        'vmo_Omon',
        'vo_Omon',
        'volo_Omon',
        'wfo_Omon',
        'wmo_Omon',
        'wo_Omon',
        'zos_Omon',
        'zossq_Omon',
        'zostoga_Omon',
        'rsds_Prim6hr',
        'rsdsdiff_Prim6hr',
        'mrso_Primday',
        'siconc_SIday',
        'siconca_SIday',
        'sisnthick_SIday',
        'sispeed_SIday',
        'sitemptop_SIday',
        'sithick_SIday',
        'sitimefrac_SIday',
        'siu_SIday',
        'siv_SIday',
        'siage_SImon',
        'siareaacrossline_SImon',
        'siarean_SImon',
        'siareas_SImon',
        'sicompstren_SImon',
        'siconc_SImon',
        'siconca_SImon',
        'sidconcdyn_SImon',
        'sidconcth_SImon',
        'sidivvel_SImon',
        'sidmassdyn_SImon',
        'sidmassevapsubl_SImon',
        'sidmassgrowthbot_SImon',
        'sidmassgrowthwat_SImon',
        'sidmasslat_SImon',
        'sidmassmeltbot_SImon',
        'sidmassmelttop_SImon',
        'sidmasssi_SImon',
        'sidmassth_SImon',
        'sidmasstranx_SImon',
        'sidmasstrany_SImon',
        'siextentn_SImon',
        'siextents_SImon',
        'sifb_SImon',
        'siflcondbot_SImon',
        'siflcondtop_SImon',
        'siflfwbot_SImon',
        'siflfwdrain_SImon',
        'sifllatstop_SImon',
        'sifllwutop_SImon',
        'siflsensupbot_SImon',
        'siflswdbot_SImon',
        'siflswdtop_SImon',
        'siflswutop_SImon',
        'sihc_SImon',
        'simass_SImon',
        'simassacrossline_SImon',
        'sipr_SImon',
        'sisaltmass_SImon',
        'sishevel_SImon',
        'sisnconc_SImon',
        'sisnhc_SImon',
        'sisnmass_SImon',
        'sisnthick_SImon',
        'sispeed_SImon',
        'sistrxdtop_SImon',
        'sistrxubot_SImon',
        'sistrydtop_SImon',
        'sistryubot_SImon',
        'sitempbot_SImon',
        'sitempsnic_SImon',
        'sitemptop_SImon',
        'sithick_SImon',
        'sitimefrac_SImon',
        'siu_SImon',
        'siv_SImon',
        'sivol_SImon',
        'sivoln_SImon',
        'sivols_SImon',
        'sndmassdyn_SImon',
        'sndmassmelt_SImon',
        'sndmasssi_SImon',
        'sndmasssnf_SImon',
        'sndmasssubl_SImon',
        'snmassacrossline_SImon',
        'clt_day',
        'hfls_day',
        'hfss_day',
        'huss_day',
        'pr_day',
        'psl_day',
        'rlut_day',
        'rsds_day',
        'sfcWindmax_day',
        'snc_day',
        'ta_day',
        'tas_day',
        'tasmax_day',
        'tasmin_day',
        'ua_day',
        'uas_day',
        'va_day',
        'vas_day',
        'zg_day',
        'areacella_fx',
        'areacellr_fx',
        'mrsofc_fx',
        'orog_fx',
        'rootd_fx',
        'sftgif_fx',
        'sftlf_fx',
        'zfull_fx',
    ]

    institute_details = {
        'id': 'CNRM-CERFACS',
        'model_ids': ['CNRM-CM6-1-HR', 'CNRM-CM6-1'],
        'calendar': CALENDAR_GREGORIAN
    }

    experiments = {
        'primWP5-amv-neg': {'start_date': datetime(1950, 1, 1),
                         'end_date': datetime(1960, 1, 1)},
        'primWP5-amv-pos': {'start_date': datetime(1950, 1, 1),
                           'end_date': datetime(1960, 1, 1)}
    }

    variant_labels = ['r{}i1p1f2'.format(i) for i in range(1, 11)]

    # activity_id
    ActivityId.objects.get_or_create(short_name=activity_id,
                                     full_name=activity_id)

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
