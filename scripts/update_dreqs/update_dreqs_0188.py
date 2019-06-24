#!/usr/bin/env python
"""
update_dreqs_0188.py

Create data requests for EC-Earth3P-HR hist-1950 r[23]i1p2f1.
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
    i1_dreqs = DataRequest.objects.filter(
        climate_model__short_name='EC-Earth3P-HR',
        experiment__short_name='hist-1950',
        rip_code='r1i1p2f1'
    )

    source_ids = ['EC-Earth3P-HR']
    clim_models = [ClimateModel.objects.get(short_name=si)
                   for si in source_ids]

    logger.debug('{} r1i1p2f1 data requests found'.
                 format(i1_dreqs.count()))

    for clim_mod in clim_models:
        for var_label in ['r2i1p2f1', 'r3i1p2f1']:
            for dreq in i1_dreqs:
                dreq.id = None
                dreq.climate_model = clim_mod
                dreq.rip_code = var_label
                dreq.save()


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
