#!/usr/bin/env python
"""
update_dreqs_0198.py

Replace 39 files from HadGEM3-GC31-HM highresSST-present r1i3p1f1 from 2006
that have been run again due to missing data in the original files.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile
from pdata_app.utils.common import delete_drs_dir

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def delete_files(query_set):
    """
    Delete any files online from the specified queryset
    """
    directories_found = []
    for df in query_set.filter(online=True):
        try:
            os.remove(os.path.join(df.directory, df.name))
        except OSError as exc:
            logger.error(str(exc))
        else:
            if df.directory not in directories_found:
                directories_found.append(df.directory)
        df.online = False
        df.directory = None
        df.save()

    for directory in directories_found:
        if not os.listdir(directory):
            delete_drs_dir(directory)
    logger.debug('{} directories removed'.format(len(directories_found)))


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
    filenames = [
        "vortmean_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200607302100.nc",
        "zg7h_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200607302100.nc",
        "va100m_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "ua50m_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "va50m_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "ua100m_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "sfcWind_Prim3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "sfcWindmax_Prim3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "evspsbl_Prim3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "hus_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200607302100.nc",
        "va_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200607302100.nc",
        "ua_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200607302100.nc",
        "wap_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200607302100.nc",
        "ta_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200607302100.nc",
        "hfls_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rlds_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "tas_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "hfss_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rsds_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rlus_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "pr_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "clt_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "ps_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "mrro_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rldscs_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rsdsdiff_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rsuscs_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rsdscs_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "prw_E3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "rsutcs_E3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "psl_E3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "prcsh_E3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "prc_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "vas_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "uas_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "prsn_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "huss_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "mrsos_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010000-200612302100.nc",
        "rsus_3hr_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200607010130-200612302230.nc",
        "vortmean_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200608010000-200608302100.nc",
        "zg7h_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200608010000-200608302100.nc",
        "hus_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200608010000-200608302100.nc",
        "va_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200608010000-200608302100.nc",
        "ua_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200608010000-200608302100.nc",
        "wap_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200608010000-200608302100.nc",
        "ta_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200608010000-200608302100.nc",
        "vortmean_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200609010000-200609302100.nc",
        "zg7h_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200609010000-200609302100.nc",
        "hus_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200609010000-200609302100.nc",
        "va_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200609010000-200609302100.nc",
        "ua_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200609010000-200609302100.nc",
        "wap_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200609010000-200609302100.nc",
        "ta_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200609010000-200609302100.nc",
        "vortmean_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200610010000-200610302100.nc",
        "zg7h_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200610010000-200610302100.nc",
        "vortmean_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200611010000-200611302100.nc",
        "zg7h_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200611010000-200611302100.nc",
        "hus_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200611010000-200611302100.nc",
        "va_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200611010000-200611302100.nc",
        "ua_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200611010000-200611302100.nc",
        "wap_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200611010000-200611302100.nc",
        "ta_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200611010000-200611302100.nc",
        "hus_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200610010000-200610302100.nc",
        "va_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200610010000-200610302100.nc",
        "ua_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200610010000-200610302100.nc",
        "wap_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200610010000-200610302100.nc",
        "ta_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200610010000-200610302100.nc",
        "vortmean_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200612010000-200612302100.nc",
        "zg7h_Prim3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200612010000-200612302100.nc",
        "hus_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200612010000-200612302100.nc",
        "va_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200612010000-200612302100.nc",
        "ua_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200612010000-200612302100.nc",
        "wap_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200612010000-200612302100.nc",
        "ta_E3hrPt_HadGEM3-GC31-HM_highresSST-present_r1i3p1f1_gn_200612010000-200612302100.nc",
    ]

    affected_files = DataFile.objects.filter(name__in=filenames)

    if affected_files.count() != 70: # no ap8_3 E3hr prw, prcsh, rsutcs or psl
        logger.error('{} affected files found'.format(affected_files.count()))
        sys.exit(1)
    else:
        logger.debug('{} affected files found'.format(affected_files.count()))

    delete_files(affected_files)
    replace_files(affected_files)


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
