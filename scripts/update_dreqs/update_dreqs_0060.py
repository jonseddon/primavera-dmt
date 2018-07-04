#!/usr/bin/env python2.7
"""
update_dreqs_0060.py

This file replaces any CMCC-CM2-VHR4 files from version v20170927
for highresSST-present and table day for hur, hus, ta, ua, va, wap and zg
for 200302, because the originals were missing data for 1st February.
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
        climate_model__short_name='CMCC-CM2-VHR4',
        experiment__short_name='highresSST-present',
        version='v20170927',
        variable_request__cmor_name__in=['hur', 'hus', 'ta', 'ua', 'va',
                                         'wap', 'zg'],
        name__contains='20030201-20030228'
    )
    logger.debug('{} files will be replaced'.format(affected_files.count()))

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
