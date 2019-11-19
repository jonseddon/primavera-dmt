#!/usr/bin/env python
"""
update_dreqs_0246.py

Replace files from EC-Earth3P hist-1950 r1i1p1f1.
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
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199101010000-199101311800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199102010000-199102281800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199103010000-199103311800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199104010000-199104301800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199107010000-199107311800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199106010000-199106301800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199105010000-199105311800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199108010000-199108311800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199110010000-199110311800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199109010000-199109301800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199111010000-199111301800.nc',
        'va1000_Prim6hrPt_EC-Earth3P_hist-1950_r1i1p1f1_gr_199112010000-199112311800.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910101-19910131.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910201-19910228.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910301-19910331.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910401-19910430.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910501-19910531.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910601-19910630.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910701-19910731.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910801-19910831.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19910901-19910930.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19911001-19911031.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19911101-19911130.nc',
        'zg_day_EC-Earth3P_hist-1950_r1i1p1f1_gr_19911201-19911231.nc'
]
    dfs = DataFile.objects.filter(name__in=affected_files)

    num_files = dfs.count()
    num_files_expected = 24
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
