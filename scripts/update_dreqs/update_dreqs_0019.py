#!/usr/bin/env python2.7
"""
update_dreqs_0019.py

This script adds the HadGEM ORCA one degree ocean model and creates coupled
data requests for it. The data requests are copied from the existing ORCA1/4
model
"""
import argparse
import logging.config
import os
import sys

from cf_units import date2num, CALENDAR_GREGORIAN

import django
django.setup()
from pdata_app.models import ClimateModel, DataRequest

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
    low_res, created = ClimateModel.objects.get_or_create(
        short_name='HadGEM3-GC31-LL', full_name='HadGEM3-GC31-LL')

    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='HadGEM3-GC31-LM',
        experiment__short_name__in=['control-1950', 'hist-1950', 'spinup-1950']
    )

    logger.debug('{} data requests found'.format(data_reqs.count()))

    for data_req in data_reqs:
        data_req.pk = None
        data_req.climate_model = low_res
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
