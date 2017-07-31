#!/usr/bin/env python2.7
"""
update_dreqs_0012.py

This script is run to update the version on two of the CNRM-CM6-1-HR to the
same as the first submission. The files for these two submissions are then
copied to the same directory and the directory value for that file is updated
to the new path.
"""
import argparse
from datetime import datetime
import logging.config
import sys

from cf_units import date2num, CALENDAR_360_DAY

import django
django.setup()

from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from pdata_app.models import DataSubmission, DataFile


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

NEW_VERSION = 'v20170623'


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
    data_subs = [
        {'dir': '/group_workspaces/jasmin2/primavera4/upload/CNRM-CERFACS/'
                'CNRM-CM6-1-HR/incoming/v20170518_1970',
         'version': 'v20170622'},
        {'dir': '/group_workspaces/jasmin2/primavera4/upload/CNRM-CERFACS/'
                'CNRM-CM6-1-HR/incoming/v20170518_1960',
         'version': 'v20170703'}
    ]

    for data_sub_dict in data_subs:
        try:
            data_sub = DataSubmission.objects.get(
                incoming_directory=data_sub_dict['dir'])
        except MultipleObjectsReturned:
            logger.error('Multiple submissions found for {}'.
                         format(data_sub_dict['dir']))
        except ObjectDoesNotExist:
            logger.error('No submissions found for {}'.
                         format(data_sub_dict['dir']))

        for data_file in data_sub.datafile_set.all():
            data_file.version




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
