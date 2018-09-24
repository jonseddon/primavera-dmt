#!/usr/bin/env python
"""
update_dreqs_0089.py

This file adds additional data_requests for the additional MOHC ensemble
member r1i1p3f2 full aerosol.
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
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'tas'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'ts'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'uas'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'vas'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'pr'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rlds'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rlus'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rsds'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rsus'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rsdscs'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rsuscs'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rldscs'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rsdt'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rsut'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rlut'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rlutcs'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'rsutcs'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'cl'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'clw'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'cli'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'ta'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'ua'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'va'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'hus'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'hur'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'wap'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'zg'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'pfull'},
        {'variable_request__table_name': 'Amon',
         'variable_request__cmor_name': 'phalf'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rlu'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rsu'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rld'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rsd'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rlucs'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rsucs'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rldcs'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'rsdcs'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'ta'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'hur'},
        {'variable_request__table_name': 'CFmon',
         'variable_request__cmor_name': 'hus'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'pr'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'tas'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rlds'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rlus'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rsds'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rsus'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'uas'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'vas'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rldscs'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rsdscs'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rsuscs'},
        {'variable_request__table_name': '3hr',
         'variable_request__cmor_name': 'rsdsdiff'}
    ]
    variant_labels = ['r1i1p3f2']
    models = ['HadGEM3-GC31-MM']

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
