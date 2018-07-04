#!/usr/bin/env python2.7
"""
update_dreqs_0067.py

This file adds additional data_requests for the additional MOHC ensemble
members.
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
    variables = [
        {'variable_request__table_name':'Amon',
         'variable_request__cmor_name': 'tas'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'pr'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'psl'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'clt'},
        {'variable_request__table_name': 'CFday',
         'variable_request__cmor_name': 'ps'},
        {'variable_request__table_name': 'day',
         'variable_request__cmor_name': 'tasmax'},
        {'variable_request__table_name': 'day',
         'variable_request__cmor_name': 'tasmin'},
        {'variable_request__table_name': 'day',
         'variable_request__cmor_name': 'pr'},
        {'variable_request__table_name': 'day',
         'variable_request__cmor_name': 'ua'},
        {'variable_request__table_name': 'day',
         'variable_request__cmor_name': 'va'},
    ]
    variant_labels = ['r1i2p1f1', 'r1i3p1f1']
    models = ['HadGEM3-GC31-LM', 'HadGEM3-GC31-MM', 'HadGEM3-GC31-HM']

    for model in models:
        for variant_label in variant_labels:
            for variable in variables:
                data_req = DataRequest.objects.get(
                    climate_model__short_name=model,
                    experiment__short_name='highresSST-present',
                    rip_code='r1i1p1f1',
                    **variable
                )
                logger.debug('{} {}'.format(variant_label, data_req))
                data_req.pk = None
                data_req.rip_code = variant_label
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
