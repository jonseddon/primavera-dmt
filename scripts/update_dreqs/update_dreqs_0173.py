#!/usr/bin/env python
"""
update_dreqs_0173.py

Update the source_id in MPI AMIP files. DON'T RUN ON LIVE DB YET!
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import argparse
import logging.config
import os
import sys

import django
django.setup()

from pdata_app.models import DataRequest
from pdata_app.utils.attribute_update import SourceIdUpdate
import pdata_site


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

TEST_DATA_DIR = '/gws/nopw/j04/primavera3/cache/jseddon/test_stream1'


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
    data_reqs = DataRequest.objects.filter(
        institute__short_name='MPI-M',
        experiment__short_name='highresSST-present',
        rip_code__in=['r1i1p1f1'],
        variable_request__table_name='Amon',
        variable_request__cmor_name__in=['tas', 'ts', 'psl']
    )
    logger.debug('{} data requests found'.format(data_reqs.count()))

    for dreq in data_reqs:
        logger.debug('Updating {}'.format(dreq))
        for data_file in dreq.datafile_set.order_by('name'):
            # TEST THAT WE ARE NOT ON THE LIVE SYSTEM
            if not data_file.directory.startswith(TEST_DATA_DIR):
                raise ValueError('File is not in {}'.format(TEST_DATA_DIR))

            logger.debug('Processing {}'.format(data_file.name))

            if data_file.climate_model.short_name == 'MPIESM-1-2-HR':
                new_source_id = 'MPI-ESM1-2-HR'
            elif data_file.climate_model.short_name == 'MPIESM-1-2-XR':
                new_source_id = 'MPI-ESM1-2-XR'
            else:
                raise ValueError('Unknown source_id {}'.
                                 format(data_file.climate_model.short_name))

            updater = SourceIdUpdate(
                os.path.join(data_file.directory, data_file.name),
                new_source_id
            )
            updater.update()
            break


if __name__ == "__main__":
    # check that we're definitely not on the live system
    if (pdata_site.settings.DATABASES['default']['HOST'] ==
            'prima-dm.ceda.ac.uk'):
        raise ValueError('This script must not be run on the live system')
    if pdata_site.settings.DATABASES['default']['HOST'] != '':
        raise ValueError('This script must not be run on the live system')
    if pdata_site.settings.DATABASES['default']['NAME'] != 'pdata_test':
        raise ValueError('This script must not be run on the live system')

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
