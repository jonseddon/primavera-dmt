#!/usr/bin/env python
"""
update_dreqs_0085.py

This file updates all CERFACS CNRM-CM6-1-HR spinup-1950 data requests to the
correct variant_label.
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
    highres_spinup = DataRequest.objects.filter(
        institute__short_name='CNRM-CERFACS',
        climate_model__short_name='CNRM-CM6-1-HR',
        experiment__short_name='spinup-1950'
    )
    logger.debug('{} affected high-res requests found'.format(highres_spinup.count()))

    highres_spinup.update(rip_code='r1i1p1f2')

    lowres_spinup = DataRequest.objects.filter(
        institute__short_name='CNRM-CERFACS',
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='spinup-1950'
    )
    logger.debug('{} affected low-res requests found'.format(lowres_spinup.count()))

    lowres_spinup.update(rip_code='r21i1p1f2')


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
