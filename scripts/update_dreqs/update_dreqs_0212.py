#!/usr/bin/env python
"""
update_dreqs_0212.py

Create dupliacte data requests for EC-Earth3P hist-1950 r3i1p2f1.
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import sys

import django
django.setup()

from pdata_app.models import DataRequest, ClimateModel


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
    # standard res
    r3_dreqs = DataRequest.objects.filter(
        climate_model__short_name='EC-Earth3P',
        experiment__short_name='hist-1950',
        rip_code='r3i1p2f1'
    )

    for vr in r3_dreqs.values_list('variable_request__cmor_name',
                                   'variable_request__table_name').distinct():
        var_dreqs = (r3_dreqs.filter(variable_request__cmor_name='vr[0]',
                                     variable_request__table_name='vr[1]').
                     order_by('id'))
        if var_dreqs.last().datafile_set.count() != 0:
            logger.error(f'{vr[0]}_{vr[1]} has files')
        else:
            var_dreqs.last.delete()
            logger.debug(f'{vr[0]}_{vr[1]} deleted')


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
