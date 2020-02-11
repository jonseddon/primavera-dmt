#!/usr/bin/env python
"""
update_dreqs_0267.py

Make a copy of the CMCC tauu and tauv variables that need to be fixed.
"""
import argparse
import logging.config
import os
import shutil
import sys

import django
django.setup()
from pdata_app.utils.common import construct_drs_path
from pdata_app.models import DataRequest

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
    copy_dir = '/gws/nopw/j04/primavera5/upload/CMCC'

    dreqs = DataRequest.objects.get(
        institute__short_name='CMCC',
        variable_request__cmor_name__in=['tauu', 'tauv']
    )

    num_dreqs = dreqs.count()
    expected_dreqs = 20
    if num_dreqs != expected_dreqs:
        logger.error(f'Found {num_dreqs} but was expecting {expected_dreqs}.')
        sys.exit(1)

    for dreq in dreqs:
        logger.debug(dreq)
        for df in dreq.datafile_set.order_by('name'):
            new_dir = os.path.join(copy_dir,
                                   construct_drs_path(df))
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
            shutil.copyfile(os.path.join(df.directory, df.name),
                            new_dir)


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
