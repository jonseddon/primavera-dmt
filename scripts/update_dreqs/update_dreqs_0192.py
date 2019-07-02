#!/usr/bin/env python
"""
update_dreqs_0192.py

This file checks through the specified data requests and checks that all of
the files are online and have the correct file size.
"""
from __future__ import unicode_literals, division, absolute_import
import argparse
import logging.config
import os
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
    data_reqs = DataRequest.objects.filter(
        climate_model__short_name='CNRM-CM6-1',
        experiment__short_name='highresSST-present',
        rip_code='r1i1p1f2',
    ).exclude(
        variable_request__table_name__startswith='Prim'
    )

    num_offline = 0
    num_not_found = 0
    num_errors = 0
    for data_req in data_reqs:
        for data_file in data_req.datafile_set.all():
            if not data_file.online:
                logger.error('Not online {}'.format(data_file.name))
                num_offline += 1
            actual_size = 0
            try:
                actual_size = os.path.getsize(
                    os.path.join(data_file.directory, data_file.name))
            except (OSError, TypeError):
                logger.error("Not found {}".format(data_file.name))
                num_not_found += 1
            if not actual_size == data_file.size:
                logger.error("Sizes don't match {}".format(data_file.name))
                num_errors += 1

    logger.debug('{} found offline'.format(num_offline))
    logger.debug('{} not found'.format(num_not_found))
    logger.debug('{} with incorrect sizes'.format(num_errors))


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
