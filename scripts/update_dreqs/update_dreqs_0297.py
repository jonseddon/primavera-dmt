#!/usr/bin/env python
"""
update_dreqs_0297.py

Remove from the DMT the CMCC rsutcs variables from
highres-future, highresSST-future and hist-1950 v20200427 that have been fixed.
"""
import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.utils.common import delete_files  # nopep8
from pdata_app.utils.replace_file import replace_files  # nopep8
from pdata_app.models import DataRequest, Settings  # nopep8

__version__ = '0.1.0b1'

DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Add additional data '
                                                 'requests')
    parser.add_argument('-l', '--log-level',
                        help='set logging level (default: %(default)s)',
                        choices=['debug', 'info', 'warning', 'error'],
                        default='info')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main():
    """
    Main entry point
    """
    dreqs = DataRequest.objects.filter(
        institute__short_name='CMCC',
        experiment__short_name__in=['highres-future', 'highresSST-future',
                                    'hist-1950'],
        variable_request__cmor_name='rsutcs',

        datafile__isnull=False
    ).distinct()

    num_dreqs = dreqs.count()
    expected_dreqs = 6
    if num_dreqs != expected_dreqs:
        logger.error(f'Found {num_dreqs} but was expecting {expected_dreqs}.')
        sys.exit(1)

    for dreq in dreqs:
        logger.info(dreq)
        delete_files(dreq.datafile_set.all(), BASE_OUTPUT_DIR, skip_badc=True)
        replace_files(dreq.datafile_set.all())


if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    log_level = getattr(logging, cmd_args.log_level.upper())

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
    main()
