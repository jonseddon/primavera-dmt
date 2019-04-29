#!/usr/bin/env python
"""
update_dreqs_0170.py

This file removes an issue from all HadGEM3-GC31 ice files explaining that
the grid in the file may be incorrect.
"""
import argparse
import logging.config
import sys


import django
django.setup()
from django.contrib.auth.models import User

from pdata_app.models import DataFile, DataIssue


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
    di = DataIssue.objects.get(id=18,
                               issue__startswith='The grid in this file may '
                                                 'be incorrect.')

    ice_files = di.data_file.filter(
        variable_request__table_name__in=['SIday', 'PrimSIday', 'SImon']
    )

    num_files_removed = ice_files.count()

    di.data_file.remove(*ice_files)

    logger.debug('Issue removed from {} files. Issue remains on {} files.'.
                 format(num_files_removed, di.data_file.count()))


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
