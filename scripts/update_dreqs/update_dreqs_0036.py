#!/usr/bin/env python2.7
"""
update_dreqs_0036.py

Remove the directory and online metadata values for some files that are about
to be deleted.
"""
import argparse
import logging.config
import os
import shutil
import sys


import django
django.setup()

from pdata_app.models import DataSubmission


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

INCOMING_DIR = '/group_workspaces/jasmin2/primavera4/upload'

DATA_SUBS = ['/group_workspaces/jasmin2/primavera4/upload/CMCC/CMCC-VHR4/20171106',
             '/group_workspaces/jasmin2/primavera4/upload/CMCC/CMCC-VHR4/20171102']


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Move files')
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
    for data_sub in DATA_SUBS:
        ds = DataSubmission.objects.get(incoming_directory=data_sub)
        ds.directory = None
        ds.save()
        for df in ds.datafile_set.filter(
                directory__startswith=
                '/group_workspaces/jasmin2/primavera4/upload'):
            df.directory = None
            df.online = False
            df.save()


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
