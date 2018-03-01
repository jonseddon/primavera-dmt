#!/usr/bin/env python2.7
"""
update_dreqs_0056.py

This file adds an issue to any MPI files from version v20171003
for hist-1950, control-1950 and spinup-1950 for both resolutions.
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
    jon = User.objects.get(username='jseddon')
    issue_txt = (
        'These simulations have been re-run with a KPP mixing scheme giving '
        'a much improved performance in the North Atlantic. The new '
        'simulations will be uploaded to JASMIN by summer-2018.'
    )
    mpi_issue = DataIssue.objects.create(issue=issue_txt, reporter=jon)
    affected_files = DataFile.objects.filter(
        data_request__institute__short_name='MPI-M',
        experiment__short_name__in=['control-1950', 'hist-1950',
                                    'spinup-1950'],
        version='v20171003'
    )
    logger.debug('{} affected files found'.format(affected_files.count()))

    mpi_issue.data_file.add(*affected_files)


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
