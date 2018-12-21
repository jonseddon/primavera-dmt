#!/usr/bin/env python
"""
update_dreqs_0115.py

This file adds additional data_requests for the additional MOHC AMIP ensemble
members r1i[2,3,14,15]p1f1.
"""
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
    existing_vars = {
        'Amon': ['clt', 'pr', 'psl', 'tas'],
        'day': ['pr', 'tasmax', 'tasmin', 'ua', 'va'],
        'CFday': ['ps'],
        '3hr': ['pr']
    }
    r1i1p1f1 = {
        'climate_model__short_name': 'HadGEM3-GC31-LM',
        'experiment__short_name': 'highresSST-present',
        'rip_code': 'r1i1p1f1'
    }


    for dr in DataRequest.objects.filter(**r1i1p1f1):
        for variant_label in ['r1i2p1f1', 'r1i3p1f1']:
            if (dr.variable_request.cmor_name not in
                    existing_vars.get(dr.variable_request.table_name, [])):
                dr.pk = None
                dr.rip_code = variant_label
                dr.save()
        for variant_label in ['r1i14p1f1', 'r1i15p1f1']:
            dr.pk = None
            dr.rip_code = variant_label
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
