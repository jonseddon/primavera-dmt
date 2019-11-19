#!/usr/bin/env python
"""
update_dreqs_0245.py

Replace files from EC-Earth3P-HR control-1950 r3i1p2f1.
"""
import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile
from pdata_app.utils.common import delete_files

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
    affected_files = [
        'psl_6hrPlev_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198910010600-198910311800.nc',
        'pr_3hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230300.nc',
        'rsds_3hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230300.nc',
        'tas_3hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'uas_3hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'vas_3hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'psl_6hrPlev_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'tas_6hrPlevPt_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'uas_6hrPlevPt_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'vas_6hrPlevPt_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'zg_6hrPlevPt_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'ps_CFday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'clt_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'hfls_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'hfss_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'hur_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'hurs_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'hursmax_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'hursmin_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'hus_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'huss_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'mrsos_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'prc_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'pr_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'prsn_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'psl_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'rlds_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'rlus_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'rlut_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'rsds_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'rsus_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'sfcWind_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'sfcWindmax_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'ta_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'tasmax_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'tasmin_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'tslsi_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'ua_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'uas_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'va_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'vas_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'zg_day_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'tauu_Eday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'tauv_Eday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'sfcWind_Prim3hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'sfcWindmax_Prim3hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230600.nc',
        'pr_Prim6hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230000.nc',
        'rsds_Prim6hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230000.nc',
        'sfcWindmax_Prim6hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230000.nc',
        'wsgmax_Prim6hr_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_198909010000-198909230000.nc',
        'mrlsl_Primday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'mrso_Primday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'prmax_Primday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'prmin_Primday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc',
        'ua_PrimdayPt_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890922.nc',
        'va_PrimdayPt_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890922.nc',
        'ts_Primday_EC-Earth3P-HR_control-1950_r3i1p2f1_gr_19890901-19890923.nc'
]
    dfs = DataFile.objects.filter(name__in=affected_files)

    num_files = dfs.count()
    num_files_expected = 57
    if num_files != num_files_expected:
        logger.error(f'{num_files} found but was expecting '
                     f'{num_files_expected}')
        sys.exit(1)

    delete_files(dfs, '/gws/nopw/j04/primavera5/stream1')
    replace_files(dfs)


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
