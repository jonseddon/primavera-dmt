#!/usr/bin/env python
"""
update_dreqs_0278.py

Make a copy of the CMCC rlut, rlutcs and rsutcs variables from
highres-future, highresSST-future and hist-1950 that need to be fixed.
"""
import argparse
import logging.config
import os
import shutil
import sys

import django
django.setup()
from pdata_app.utils.common import construct_drs_path  # nopep8
from pdata_app.models import DataRequest  # nopep8

__version__ = '0.1.0b1'

DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


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


def main(args):
    """
    Main entry point
    """
    copy_dir = '/gws/nopw/j04/primavera5/upload/CMCC/fluxes'

    dreqs = DataRequest.objects.filter(
        institute__short_name='CMCC',
        experiment__short_name__in=['highres-future', 'highresSST-future',
                                    'hist-1950'],
        variable_request__cmor_name__in=['rlut', 'rlutcs', 'rsutcs'],
        datafile__isnull=False
    ).distinct()

    num_dreqs = dreqs.count()
    expected_dreqs = 18
    if num_dreqs != expected_dreqs:
        logger.error(f'Found {num_dreqs} but was expecting {expected_dreqs}.')
        sys.exit(1)

    for dreq in dreqs:
        logger.info(dreq)
        for df in dreq.datafile_set.order_by('name'):
            new_dir = os.path.join(copy_dir,
                                   construct_drs_path(df))
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
            shutil.copyfile(os.path.join(df.directory, df.name),
                            os.path.join(new_dir, df.name))


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
    main(cmd_args)
