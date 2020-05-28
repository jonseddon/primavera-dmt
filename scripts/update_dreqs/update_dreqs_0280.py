#!/usr/bin/env python
"""
update_dreqs_0280.py

This file creates data requests for a few more variables from MOHC -MH
hist-1950, control-1950 and highres-future.
"""
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
    mh = ClimateModel.objects.get(short_name='HadGEM3-GC31-MH')

    vars = [
        {'variable_request__table_name': 'day',
         'variable_request__cmor_name__in': ['y'
                                             'ta', 'ua', 'va', 'wap']}
    ]

    expts = ['hist-1950', 'control-1950', 'highres-future']

    for var in vars:
        for expt in expts:
            r1i1p1f1 = {
                'climate_model__short_name': 'HadGEM3-GC31-HM',
                'experiment__short_name': expt,
                'rip_code': 'r1i1p1f1'
            }

            for dr in DataRequest.objects.filter(**var, **r1i1p1f1):
                dr.pk = None
                dr.climate_model = mh
                dr.save()


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
