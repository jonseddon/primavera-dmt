#!/usr/bin/env python
"""
update_dreqs_0168.py

This file adds an issue to all HadGEM3-GC31 ocean or ice files explaining that
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
    jon = User.objects.get(username='jseddon')
    nemo_txt = (
        'The grid in this file may be incorrect. For each variable the T, U, '
        'V or W may have been included in this file irrespective of the grid '
        'that the data was generated on. It is not currently known which grid '
        'has been included (although this can be determined easily). Please '
        'contact Jon or Malcolm for assistance with these files.'
    )
    nemo_issue, _created = DataIssue.objects.get_or_create(issue=nemo_txt,
                                                           reporter=jon)

    nemo_files = DataFile.objects.filter(
        institute__short_name__in=['MOHC', 'NERC'],
        experiment__short_name__in=['control-1950', 'hist-1950', 'spinup-1950',
                                    'highres-future'],
        variable_request__table_name__in=['SIday', 'PrimSIday', 'SImon',
                                          'Oday', 'PrimOday', 'Omon',
                                          'PrimOmon']
    )
    logger.debug('{} files requiring issue found'.format(nemo_files.count()))
    nemo_issue.data_file.add(*nemo_files)


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
