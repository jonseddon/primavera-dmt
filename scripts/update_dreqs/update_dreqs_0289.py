#!/usr/bin/env python
"""
update_dreqs_0289.py

Remove the ocean and ice uv and w grid variables v20200514 HadGEM3-GC31-HH
coupled data that has been regenerated for a second time.
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

    common_queries = {
        'climate_model__short_name': 'HadGEM3-GC31-HH',
        'experiment__short_name__in': ['control-1950', 'highres-future',
                                        'hist-1950'],
        'version': 'v20200514'
    }

    omon = DataFile.objects.filter(
        **common_queries,
        variable_request__table_name = 'Omon',
        variable_request__cmor_name__in = ['tauuo', 'tauvo', 'umo', 'uo', 'vmo',
                                           'vo', 'wmo']
    )
    primomon = DataFile.objects.filter(
        **common_queries,
        variable_request__table_name = 'PrimOmon',
        variable_request__cmor_name__in = ['u2o', 'uso', 'uto', 'v2o', 'vso',
                                           'vto', 'wo']
    )
    primoday = DataFile.objects.filter(
        **common_queries,
        variable_request__table_name = 'PrimOday',
        variable_request__cmor_name__in = ['tauuo', 'tauvo', 'uo', 'vo']
    )

    affected_files = omon | primomon | primoday

    num_files = affected_files.count()
    logger.debug(f'{num_files} affected files found')

    # delete_files(affected_files, '/gws/nopw/j04/primavera5/stream1',
    #              skip_badc=True)
    # replace_files(affected_files)


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
