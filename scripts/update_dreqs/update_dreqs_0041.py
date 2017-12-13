#!/usr/bin/env python2.7
"""
update_dreqs_0041.py

This script adds NERC as an institute and changes the institute for
HadGEM3-GC31-HH control-1950 data requests from MOHC to NERC.
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.models import Institute, DataRequest

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
    nerc, created = Institute.objects.get_or_create(
        short_name='NERC',
        full_name='Natural Environment Research Council, STFC-RAL, Harwell, '
                  'Oxford, OX11 0QX, UK'
    )

    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-HH',
        experiment__short_name='control-1950'
    )

    logger.debug('{} data requests found'.format(data_reqs.count()))

    for data_req in data_reqs:
        data_req.institute = nerc
        data_req.save()


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
