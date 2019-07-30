#!/usr/bin/env python
"""
update_dreqs_0208.py

This moves the files in two duplicate data submissions to replaced files.
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

from pdata_app.models import DataSubmission, Settings
from pdata_app.utils.common import delete_files
from pdata_app.utils.replace_file import replace_files


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
    dss = DataSubmission.objects.filter(incoming_directory__in=[
        '/gws/nopw/j04/primavera4/upload/EC-Earth-Consortium/EC-Earth3P-HR/'
        'control-1950/r3i1p2f1/1950-1952/SeaIce',
        '/gws/nopw/j04/primavera4/upload/EC-Earth-Consortium/EC-Earth3P-HR/'
        'control-1950/r3i1p2f1/1950-1952/Rest'
    ])
    
    base_output_dir = Settings.get_solo().base_output_dir

    for ds in dss:
        delete_files(ds.datafile_set.all(), base_output_dir)
        replace_files(ds.datafile_set.all())



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
