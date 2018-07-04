#!/usr/bin/env python2.7
"""
update_dreqs_0059.py

This file replaces ECMWF ECMWF-IFS-LR files from version v20170915
for hist-1950, control-1950 and spinup-1950.
"""
import argparse
import logging.config
import sys


import django
django.setup()

from pdata_app.models import DataFile
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
    parser.add_argument('-t', '--test', help='test-run; display the number of '
                                             'files identified but do not '
                                             'replace them',
                        action='store_true')
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
    affected_files = DataFile.objects.filter(
        data_request__institute__short_name='ECMWF',
        data_request__climate_model__short_name='ECMWF-IFS-LR',
        experiment__short_name__in=['control-1950', 'hist-1950',
                                    'spinup-1950'],
        version='v20170915'
    )
    logger.debug('{} affected files found'.format(affected_files.count()))

    if not args.test:
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
