#!/usr/bin/env python2.7
"""
update_dreqs_0017.py

This script is run to change the version string in all files from the specified
CMCC submissions to one value.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()

from pdata_app.models import DataSubmission

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
    CMCC_NEW_VERSION = 'v20170706'

    wrong_versions = [
        u'/group_workspaces/jasmin2/primavera4/upload/CMCC/20170708',
        u'/group_workspaces/jasmin2/primavera4/upload/CMCC/20170725',
    ]

    for incom_dir in wrong_versions:
        logger.debug(incom_dir)
        data_sub = DataSubmission.objects.get(incoming_directory=incom_dir)
        for data_file in data_sub.datafile_set.all():
            data_file.version = CMCC_NEW_VERSION
            data_file.save()


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
