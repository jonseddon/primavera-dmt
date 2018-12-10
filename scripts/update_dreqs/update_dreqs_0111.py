#!/usr/bin/env python
"""
update_dreqs_0111.py

This script removes duplicate data requests for HadGEM3-GC31-LL highres-future.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import sys

import django
django.setup()

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
    dupe_vars = [
        'uo_PrimOday',
        'vo_PrimOday',
        'ficeberg_Omon'
    ]

    for var in dupe_vars:
        cmor_name, table_name = var.split('_')
        dreqs = DataRequest.objects.filter(
            climate_model__short_name='HadGEM3-GC31-LL',
            experiment__short_name='highres-future',
            rip_code='r1i1p1f1',
            variable_request__cmor_name=cmor_name,
            variable_request__table_name=table_name
        )
        if dreqs.count() == 0:
            logger.error('No dreqs found for {}'.format(var))
        elif dreqs.count() == 1:
            logger.warning('1 dreq found for {}'.format(var))
        elif dreqs.count() == 2:
            dreqs.last().delete()
            logger.debug('Dreq deleted for {}'.format(var))
        else:
            logger.error('{} dreq found for {}'.format(dreqs.count(),
                                                       var))


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
