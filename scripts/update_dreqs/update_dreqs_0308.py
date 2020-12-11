#!/usr/bin/env python
"""
update_dreqs_0308.py

Remove from the DMT the EC-Earth E3hrPt psl, ua850 and va850 variables
as these are not in the data request and cannot be published.
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
        institute__short_name='EC-Earth-Consortium',
        variable_request__table_name='E3hrPt',
        variable_request__cmor_name__in=['psl', 'ua850', 'va850'],
        datafile__isnull=False
    ).distinct()

    num_dreqs = dreqs.count()
    expected_dreqs = 16
    if num_dreqs != expected_dreqs:
        logger.error(f'Found {num_dreqs} but was expecting {expected_dreqs}.')
        sys.exit(1)

    for dreq in dreqs:
        logger.info(dreq)
        delete_files(dreq.datafile_set.all(), BASE_OUTPUT_DIR)
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
