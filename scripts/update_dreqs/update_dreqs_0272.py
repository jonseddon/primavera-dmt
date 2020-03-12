#!/usr/bin/env python
"""
update_dreqs_0272.py

Remove from the DMT the CMCC tauu and tauv variables that have been fixed.
"""
import argparse
import logging.config
import os
import shutil
import sys

import django
django.setup()
from pdata_app.utils.common import construct_drs_path, delete_files
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataRequest, Settings

__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir


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
    copy_dir = '/gws/nopw/j04/primavera5/upload/CMCC'

    dreqs = DataRequest.objects.filter(
        institute__short_name='CMCC',
        variable_request__cmor_name__in=['tauu', 'tauv'],
        datafile__isnull=False
    ).distinct()

    num_dreqs = dreqs.count()
    expected_dreqs = 20
    if num_dreqs != expected_dreqs:
        logger.error(f'Found {num_dreqs} but was expecting {expected_dreqs}.')
        sys.exit(1)

    for dreq in dreqs:
        logger.debug(dreq)
        delete_files(dreq.datafile_set.all(), BASE_OUTPUT_DIR, skip_badc=True)
        replace_files(dreq.datafile_set.all())


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
