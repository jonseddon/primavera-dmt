#!/usr/bin/env python
"""
update_dreqs_0314.py

Reset the data request on MPI HR highresSST-present data requests
"""
import argparse
import logging.config
import os
import sys

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
    parser = argparse.ArgumentParser(description='Create retrieval requests')
    parser.add_argument('-l', '--log-level',
                        help='set logging level (default: %(default)s)',
                        choices=['debug', 'info', 'warning', 'error'],
                        default='warning')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    dreqs = DataRequest.objects.filter(
        institute__short_name='MPI-M',
        climate_model__short_name='MPI-ESM1-2-HR',
        experiment__short_name='highresSST-present',
        variable_request__table_name__startswith='Prim',
        datafile__isnull=False
    ).distinct()

    dreq_cmpts = list(dreqs.values_list('variable_request__table_name',
                                        'variable_request__cmor_name'))

    new_cm = ClimateModel.objects.get(short_name='MPIESM-1-2-HR')
    for dreq_cmpt in dreq_cmpts:
        current_dreq = DataRequest.objects.get(
            institute__short_name='MPI-M',
            climate_model__short_name='MPI-ESM1-2-HR',
            experiment__short_name='highresSST-present',
            variable_request__table_name=dreq_cmpt[0],
            variable_request__cmor_name=dreq_cmpt[1],
        )
        logger.info(current_dreq)
        new_dreq = DataRequest.objects.get(
            institute__short_name='MPI-M',
            climate_model__short_name='MPIESM-1-2-HR',
            experiment__short_name='highresSST-present',
            variable_request__table_name=dreq_cmpt[0],
            variable_request__cmor_name=dreq_cmpt[1],
        )
        for df in current_dreq.datafile_set.order_by('name'):
            df.name = df.incoming_name
            df.data_request = new_dreq
            df.climate_model = new_cm
            df.save()


if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    log_level = getattr(logging, cmd_args.log_level.upper())

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
