#!/usr/bin/env python
"""
update_dreqs_0249.py

Replace files from EC-Earth3P [uv]a850_E3hrPt EC-Earth3P_control-1950_r1i1p1f1,
EC-Earth3P-HR_hist-1950_r1i1p1f1 and EC-Earth3P-HR_control-1950_r1i1p1f1.
"""
import argparse
import logging.config
import sys

import django
django.setup()
from pdata_app.utils.replace_file import replace_files
from pdata_app.models import DataFile
from pdata_app.utils.common import delete_files

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
    dfs = DataFile.objects.filter(
        climate_model__short_name__in=['EC-Earth3P', 'EC-Earth3P-HR'],
        experiment__short_name='control-1950',
        rip_code='r1i1p1f1',
        variable_request__table_name='E3hrPt',
        variable_request__cmor_name__in=['ua850', 'va850']
    )

    num_files = dfs.count()
    num_files_expected = 4633
    if num_files != num_files_expected:
        logger.error(f'{num_files} found but was expecting '
                     f'{num_files_expected}')
        sys.exit(1)

    delete_files(dfs, '/gws/nopw/j04/primavera5/stream1')
    replace_files(dfs)

    dfs = DataFile.objects.filter(
        climate_model__short_name='EC-Earth3P-HR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p1f1',
        variable_request__table_name='E3hrPt',
        variable_request__cmor_name__in=['ua850', 'va850']
    )

    num_files = dfs.count()
    num_files_expected = 1508
    if num_files != num_files_expected:
        logger.error(f'{num_files} found but was expecting '
                     f'{num_files_expected}')
        sys.exit(1)

    delete_files(dfs, '/gws/nopw/j04/primavera5/stream1')
    replace_files(dfs)


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
